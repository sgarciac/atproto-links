use jetstream::{
    events::{Cursor, EventKind, JetstreamEvent},
    exports::{Did, Nsid},
    DefaultJetstreamEndpoints, JetstreamCompression, JetstreamConfig, JetstreamConnector,
    JetstreamReceiver,
};
use std::mem;
use std::time::Duration;
use tokio::sync::mpsc::{channel, Receiver, Sender};

use crate::error::FirehoseEventError;
use crate::{DeleteAccount, EventBatch, UFOsCommit};

const MAX_BATCHED_RECORDS: usize = 128; // *non-blocking* limit. drops oldest batched record per collection once reached.
const MAX_ACCOUNT_REMOVES: usize = 1024; // hard limit, extremely unlikely to reach, but just in case
const MAX_BATCHED_COLLECTIONS: usize = 64; // hard limit, MAX_BATCHED_RECORDS applies per-collection
const MIN_BATCH_SPAN_SECS: f64 = 2.; // breathe
const MAX_BATCH_SPAN_SECS: f64 = 60.; // hard limit, pause consumer if we're unable to send by now
const SEND_TIMEOUT_S: f64 = 15.; // if the channel is blocked longer than this, something is probably up
const BATCH_QUEUE_SIZE: usize = 1; // nearly-rendez-vous

#[derive(Debug, Default)]
struct CurrentBatch {
    initial_cursor: Option<Cursor>,
    batch: EventBatch,
}

#[derive(Debug)]
struct Batcher {
    jetstream_receiver: JetstreamReceiver,
    batch_sender: Sender<EventBatch>,
    current_batch: CurrentBatch,
}

pub async fn consume(
    jetstream_endpoint: &str,
    cursor: Option<Cursor>,
    no_compress: bool,
) -> anyhow::Result<Receiver<EventBatch>> {
    let endpoint = DefaultJetstreamEndpoints::endpoint_or_shortcut(jetstream_endpoint);
    if endpoint == jetstream_endpoint {
        log::info!("connecting to jetstream at {endpoint}");
    } else {
        log::info!("connecting to jetstream at {jetstream_endpoint} => {endpoint}");
    }
    let config: JetstreamConfig = JetstreamConfig {
        endpoint,
        compression: if no_compress {
            JetstreamCompression::None
        } else {
            JetstreamCompression::Zstd
        },
        replay_on_reconnect: true,
        channel_size: 1024, // buffer up to ~1s of jetstream events
        ..Default::default()
    };
    let jetstream_receiver = JetstreamConnector::new(config)?
        .connect_cursor(cursor)
        .await?;
    let (batch_sender, batch_reciever) = channel::<EventBatch>(BATCH_QUEUE_SIZE);
    let mut batcher = Batcher::new(jetstream_receiver, batch_sender);
    tokio::task::spawn(async move { batcher.run().await });
    Ok(batch_reciever)
}

impl Batcher {
    fn new(jetstream_receiver: JetstreamReceiver, batch_sender: Sender<EventBatch>) -> Self {
        Self {
            jetstream_receiver,
            batch_sender,
            current_batch: Default::default(),
        }
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        loop {
            if let Some(event) = self.jetstream_receiver.recv().await {
                self.handle_event(event).await?
            } else {
                anyhow::bail!("channel closed");
            }
        }
    }

    async fn handle_event(&mut self, event: JetstreamEvent) -> anyhow::Result<()> {
        if let Some(earliest) = &self.current_batch.initial_cursor {
            if event.cursor.duration_since(earliest)? > Duration::from_secs_f64(MAX_BATCH_SPAN_SECS)
            {
                self.send_current_batch_now().await?;
            }
        } else {
            self.current_batch.initial_cursor = Some(event.cursor);
        }

        match event.kind {
            EventKind::Commit => {
                let commit = event
                    .commit
                    .ok_or(FirehoseEventError::CommitEventMissingCommit)?;
                let (commit, nsid) = UFOsCommit::from_commit_info(commit, event.did, event.cursor)?;
                self.handle_commit(commit, nsid).await?;
            }
            EventKind::Account => {
                let account = event
                    .account
                    .ok_or(FirehoseEventError::AccountEventMissingAccount)?;
                if !account.active {
                    self.handle_delete_account(event.did, event.cursor).await?;
                }
            }
            _ => {}
        }

        // if the queue is empty and we have enough, send immediately. otherewise, let the current batch fill up.
        if let Some(earliest) = &self.current_batch.initial_cursor {
            if event.cursor.duration_since(earliest)?.as_secs_f64() > MIN_BATCH_SPAN_SECS
                && self.batch_sender.capacity() == BATCH_QUEUE_SIZE
            {
                log::info!("queue empty: immediately sending batch.");
                self.send_current_batch_now().await?;
            }
        }
        Ok(())
    }

    async fn handle_commit(&mut self, commit: UFOsCommit, nsid: Nsid) -> anyhow::Result<()> {
        if !self.current_batch.batch.commits_by_nsid.contains_key(&nsid)
            && self.current_batch.batch.commits_by_nsid.len() >= MAX_BATCHED_COLLECTIONS
        {
            self.send_current_batch_now().await?;
        }

        self.current_batch
            .batch
            .commits_by_nsid
            .entry(nsid)
            .or_default()
            .truncating_insert(commit, MAX_BATCHED_RECORDS);

        Ok(())
    }

    async fn handle_delete_account(&mut self, did: Did, cursor: Cursor) -> anyhow::Result<()> {
        if self.current_batch.batch.account_removes.len() >= MAX_ACCOUNT_REMOVES {
            self.send_current_batch_now().await?;
        }
        self.current_batch
            .batch
            .account_removes
            .push(DeleteAccount { did, cursor });
        Ok(())
    }

    // holds up all consumer progress until it can send to the channel
    // use this when the current batch is too full to add more to it
    async fn send_current_batch_now(&mut self) -> anyhow::Result<()> {
        log::info!(
            "attempting to send batch now (capacity: {})",
            self.batch_sender.capacity()
        );
        let current = mem::take(&mut self.current_batch);
        self.batch_sender
            .send_timeout(current.batch, Duration::from_secs_f64(SEND_TIMEOUT_S))
            .await?;
        Ok(())
    }
}

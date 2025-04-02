use jetstream::{
    events::{CommitEvent, CommitOp, Cursor, EventKind, JetstreamEvent},
    exports::Did,
    DefaultJetstreamEndpoints, JetstreamCompression, JetstreamConfig, JetstreamConnector,
    JetstreamReceiver,
};
use std::mem;
use std::time::Duration;
use tokio::sync::mpsc::{channel, Receiver, Sender};

use crate::{CreateRecord, DeleteAccount, DeleteRecord, EventBatch, ModifyRecord, UpdateRecord};

const MAX_BATCHED_RECORDS: usize = 64; // *non-blocking* limit. drops oldest batched record per collection once reached.
const MAX_BATCHED_MODIFIES: usize = 32; // hard limit, total updates and deletes across all collections.
const MAX_ACCOUNT_REMOVES: usize = 128; // hard limit, total account deletions. actually the least frequent event, but tiny.
const MAX_BATCHED_COLLECTIONS: usize = 32; // hard limit, MAX_BATCHED_RECORDS applies per collection
const MIN_BATCH_SPAN_SECS: f64 = 2.; // try to get a bit of rest a bit.
const MAX_BATCH_SPAN_SECS: f64 = 60.; // hard limit of duration from oldest to latest event cursor within a batch, in seconds.

const SEND_TIMEOUT_S: f64 = 60.;
const BATCH_QUEUE_SIZE: usize = 64; // 4096 got OOM'd. update: 1024 also got OOM'd during L0 compaction blocking

#[derive(Debug)]
struct Batcher {
    jetstream_receiver: JetstreamReceiver,
    batch_sender: Sender<EventBatch>,
    current_batch: EventBatch,
}

pub async fn consume(
    jetstream_endpoint: &str,
    cursor: Option<Cursor>,
    no_compress: bool,
) -> anyhow::Result<Receiver<EventBatch>> {
    let endpoint = DefaultJetstreamEndpoints::endpoint_or_shortcut(jetstream_endpoint);
    if endpoint == jetstream_endpoint {
        eprintln!("connecting to jetstream at {endpoint}");
    } else {
        eprintln!("connecting to jetstream at {jetstream_endpoint} => {endpoint}");
    }
    let config: JetstreamConfig = JetstreamConfig {
        endpoint,
        compression: if no_compress {
            JetstreamCompression::None
        } else {
            JetstreamCompression::Zstd
        },
        replay_on_reconnect: true,
        channel_size: 64, // small because we expect to be fast....?
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
        let event_cursor = event.cursor;

        if let Some(earliest) = &self.current_batch.first_jetstream_cursor {
            if event_cursor.duration_since(earliest)? > Duration::from_secs_f64(MAX_BATCH_SPAN_SECS)
            {
                self.send_current_batch_now().await?;
            }
        } else {
            self.current_batch.first_jetstream_cursor = Some(event_cursor.clone());
        }

        match event.kind {
            EventKind::Commit if event.commit.is_some() => {
                let commit = event.commit.unwrap();
                match commit.operation {
                    CommitOp::Create => {
                        self.handle_create_record(event.did, commit, event_cursor.clone())
                            .await?;
                    }
                    CommitOp::Update => {
                        self.handle_modify_record(modify_update(
                            event.did,
                            commit,
                            event_cursor.clone(),
                        ))
                        .await?;
                    }
                    CommitOp::Delete => {
                        self.handle_modify_record(modify_delete(
                            event.did,
                            commit,
                            event_cursor.clone(),
                        ))
                        .await?;
                    }
                }
            }
            EventKind::Account if event.account.is_some() => {
                let account = event.account.unwrap();
                if !account.active {
                    self.handle_remove_account(account.did, event_cursor.clone())
                        .await?;
                }
            }
            _ => {}
        };
        self.current_batch.last_jetstream_cursor = Some(event_cursor.clone());

        // if the queue is empty and we have enough, send immediately. otherewise, let the current batch fill up.
        if let Some(earliest) = &self.current_batch.first_jetstream_cursor {
            if event_cursor.duration_since(earliest)?.as_secs_f64() > MIN_BATCH_SPAN_SECS
                && self.batch_sender.capacity() == BATCH_QUEUE_SIZE
            {
                log::trace!("queue empty: immediately sending batch.");
                if let Err(send_err) = self
                    .batch_sender
                    .send(mem::take(&mut self.current_batch))
                    .await
                {
                    anyhow::bail!("Could not send batch, likely because the receiver closed or dropped: {send_err:?}");
                }
            }
        }
        Ok(())
    }

    // holds up all consumer progress until it can send to the channel
    // use this when the current batch is too full to add more to it
    async fn send_current_batch_now(&mut self) -> anyhow::Result<()> {
        log::warn!(
            "attempting to send batch now (capacity: {})",
            self.batch_sender.capacity()
        );
        self.batch_sender
            .send_timeout(
                mem::take(&mut self.current_batch),
                Duration::from_secs_f64(SEND_TIMEOUT_S),
            )
            .await?;
        Ok(())
    }

    async fn handle_create_record(
        &mut self,
        did: Did,
        commit: CommitEvent,
        cursor: Cursor,
    ) -> anyhow::Result<()> {
        if !self
            .current_batch
            .record_creates
            .contains_key(&commit.collection)
            && self.current_batch.record_creates.len() >= MAX_BATCHED_COLLECTIONS
        {
            self.send_current_batch_now().await?;
        }
        let record = serde_json::from_str(commit.record.unwrap().get())?;
        let record = CreateRecord {
            did,
            rkey: commit.rkey,
            record,
            cursor,
        };
        let collection = self
            .current_batch
            .record_creates
            .entry(commit.collection)
            .or_default();
        collection.total_seen += 1;
        collection.samples.push_front(record);
        collection.samples.truncate(MAX_BATCHED_RECORDS);
        Ok(())
    }

    async fn handle_modify_record(&mut self, modify_record: ModifyRecord) -> anyhow::Result<()> {
        if self.current_batch.record_modifies.len() >= MAX_BATCHED_MODIFIES {
            self.send_current_batch_now().await?;
        }
        self.current_batch.record_modifies.push(modify_record);
        Ok(())
    }

    async fn handle_remove_account(&mut self, did: Did, cursor: Cursor) -> anyhow::Result<()> {
        if self.current_batch.account_removes.len() >= MAX_ACCOUNT_REMOVES {
            self.send_current_batch_now().await?;
        }
        self.current_batch
            .account_removes
            .push(DeleteAccount { did, cursor });
        Ok(())
    }
}

fn modify_update(did: Did, commit: CommitEvent, cursor: Cursor) -> ModifyRecord {
    let record = serde_json::from_str(commit.record.unwrap().get()).unwrap();
    ModifyRecord::Update(UpdateRecord {
        did,
        collection: commit.collection,
        rkey: commit.rkey,
        record,
        cursor,
    })
}

fn modify_delete(did: Did, commit: CommitEvent, cursor: Cursor) -> ModifyRecord {
    ModifyRecord::Delete(DeleteRecord {
        did,
        collection: commit.collection,
        rkey: commit.rkey,
        cursor,
    })
}

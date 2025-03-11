use jetstream::{
    events::{
        account::AccountEvent,
        commit::{CommitData, CommitEvent, CommitInfo},
        Cursor, EventInfo, JetstreamEvent,
    },
    exports::Did,
    DefaultJetstreamEndpoints, JetstreamCompression, JetstreamConfig, JetstreamConnector,
    JetstreamReceiver,
};
use std::mem;
use std::time::Duration;
use tokio::sync::mpsc::{channel, error::TrySendError, Receiver, Sender};

use crate::{DeleteRecord, EventBatch, SetRecord};

const MAX_BATCHED_RECORDS: usize = 2048; // non-blocking limit. drops oldest batched record per collection.
const MAX_BATCHED_COLLECTIONS: usize = 512; // block at this point. pretty arbitrary, limit unbounded growth since during replay it could grow a lot.
const MAX_BATCHED_DELETES: usize = 1024; // block at this point. fairly arbitrary, limit unbounded.
const MAX_ACCOUNT_REMOVES: usize = 1024; // block at this point. very arbirary.

const SEND_TIMEOUT_S: f64 = 4.;

#[derive(Debug)]
struct Batcher {
    jetstream_receiver: JetstreamReceiver<serde_json::Value>,
    batch_sender: Sender<EventBatch>,
    current_batch: EventBatch,
}

pub async fn consume(
    jetstream_endpoint: &str,
    cursor: Option<Cursor>,
) -> anyhow::Result<Receiver<EventBatch>> {
    let config: JetstreamConfig<serde_json::Value> = JetstreamConfig {
        endpoint: DefaultJetstreamEndpoints::endpoint_or_shortcut(jetstream_endpoint),
        compression: JetstreamCompression::Zstd,
        ..Default::default()
    };
    let jetstream_receiver = JetstreamConnector::new(config)?
        .connect_cursor(cursor)
        .await?;
    let (batch_sender, batch_reciever) = channel::<EventBatch>(1); // *almost* rendezvous: one message in the middle
    let mut batcher = Batcher::new(jetstream_receiver, batch_sender);
    tokio::task::spawn(async move { batcher.run().await });
    Ok(batch_reciever)
}

impl Batcher {
    fn new(
        jetstream_receiver: JetstreamReceiver<serde_json::Value>,
        batch_sender: Sender<EventBatch>,
    ) -> Self {
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

    async fn handle_event(
        &mut self,
        event: JetstreamEvent<serde_json::Value>,
    ) -> anyhow::Result<()> {
        let event_cursor = event.cursor();
        match event {
            JetstreamEvent::Commit(CommitEvent::Create { commit, info }) => {
                self.handle_set_record(true, commit, info).await?
            }
            JetstreamEvent::Commit(CommitEvent::Update { commit, info }) => {
                self.handle_set_record(false, commit, info).await?
            }
            JetstreamEvent::Commit(CommitEvent::Delete { commit, info }) => {
                self.handle_delete_record(commit, info).await?
            }
            JetstreamEvent::Account(AccountEvent { info, account }) if !account.active => {
                self.handle_remove_account(info.did).await?
            }
            JetstreamEvent::Account(_) => {} // ignore account *activations*
            JetstreamEvent::Identity(_) => {} // identity events are noops for us
        };
        self.current_batch.last_jetstream_cursor = Some(event_cursor);

        match self.batch_sender.try_reserve() {
            Ok(permit) => permit.send(mem::take(&mut self.current_batch)),
            Err(TrySendError::Full(())) => {} // no worries if not, keep batching while waiting for capacity
            Err(TrySendError::Closed(())) => anyhow::bail!("batch channel closed"),
        }
        Ok(())
    }

    // holds up all consumer progress until it can send to the channel
    // use this when the current batch is too full to add more to it
    async fn send_current_batch_now(&mut self) -> anyhow::Result<()> {
        self.batch_sender
            .send_timeout(
                mem::take(&mut self.current_batch),
                Duration::from_secs_f64(SEND_TIMEOUT_S),
            )
            .await?;
        Ok(())
    }

    async fn handle_set_record(
        &mut self,
        new: bool,
        commit: CommitData<serde_json::Value>,
        info: EventInfo,
    ) -> anyhow::Result<()> {
        if !self
            .current_batch
            .records
            .contains_key(&commit.info.collection)
            && self.current_batch.records.len() >= MAX_BATCHED_COLLECTIONS
        {
            self.send_current_batch_now().await?;
        }
        let record = SetRecord {
            new,
            did: info.did,
            rkey: commit.info.rkey,
            record: commit.record,
        };
        let collection = self
            .current_batch
            .records
            .entry(commit.info.collection)
            .or_default();
        collection.total_seen += 1;
        collection.samples.push_front(record);
        collection.samples.truncate(MAX_BATCHED_RECORDS);
        Ok(())
    }

    async fn handle_delete_record(
        &mut self,
        commit_info: CommitInfo,
        info: EventInfo,
    ) -> anyhow::Result<()> {
        if self.current_batch.record_deletes.len() >= MAX_BATCHED_DELETES {
            self.send_current_batch_now().await?;
        }
        let rm = DeleteRecord {
            did: info.did,
            collection: commit_info.collection,
            rkey: commit_info.rkey,
        };
        self.current_batch.record_deletes.push(rm);
        Ok(())
    }

    async fn handle_remove_account(&mut self, did: Did) -> anyhow::Result<()> {
        if self.current_batch.account_removes.len() >= MAX_ACCOUNT_REMOVES {
            self.send_current_batch_now().await?;
        }
        self.current_batch.account_removes.push(did);
        Ok(())
    }
}

use jetstream::exports::Did;
use jetstream::{
    events::{
        account::AccountEvent,
        commit::{CommitData, CommitEvent, CommitInfo},
        EventInfo, JetstreamEvent,
    },
    DefaultJetstreamEndpoints, JetstreamCompression, JetstreamConfig, JetstreamConnector,
    JetstreamReceiver,
};
use std::mem;
use std::time::Duration;
use tokio::sync::mpsc::{channel, error::TrySendError, Receiver, Sender};

use crate::{DeleteRecord, EventBatch, SetRecord};

const MAX_BATCHED_RECORDS: usize = 32; // non-blocking limit. drops oldest batched record per collection.
const MAX_BATCHED_COLLECTIONS: usize = 256; // block at this point. pretty arbitrary, limit unbounded growth since during replay it could grow a lot.
const MAX_BATCHED_DELETES: usize = 1024; // block at this point. fairly arbitrary, limit unbounded.
const MAX_ACCOUNT_REMOVES: usize = 1; // block at this point. these can be heavy so hold at each one.

const SEND_TIMEOUT_S: f64 = 3.;

#[derive(Debug)]
struct Batcher {
    jetstream_receiver: JetstreamReceiver<serde_json::Value>,
    batch_sender: Sender<EventBatch>,
    current_batch: EventBatch,
}

pub async fn consume(jetstream_endpoint: &str) -> anyhow::Result<Receiver<EventBatch>> {
    let config: JetstreamConfig<serde_json::Value> = JetstreamConfig {
        endpoint: DefaultJetstreamEndpoints::endpoint_or_shortcut(jetstream_endpoint),
        compression: JetstreamCompression::Zstd,
        ..Default::default()
    };
    let jetstream_receiver = JetstreamConnector::new(config)?.connect().await?;
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
        let batch_full = match event {
            JetstreamEvent::Commit(CommitEvent::Create { commit, info }) => {
                self.handle_set_record(true, commit, info)
            }
            JetstreamEvent::Commit(CommitEvent::Update { commit, info }) => {
                self.handle_set_record(false, commit, info)
            }
            JetstreamEvent::Commit(CommitEvent::Delete { commit, info }) => {
                self.handle_delete_record(commit, info)
            }
            JetstreamEvent::Identity(_) => false, // identity events are noops for us
            JetstreamEvent::Account(AccountEvent { info, account }) if !account.active => {
                self.handle_remove_account(info.did)
            }
            JetstreamEvent::Account(_) => false, // ignore account *activations*
        };
        if batch_full {
            self.batch_sender
                .send_timeout(
                    mem::take(&mut self.current_batch),
                    Duration::from_secs_f64(SEND_TIMEOUT_S),
                )
                .await?;
        } else {
            match self.batch_sender.try_reserve() {
                Ok(permit) => permit.send(mem::take(&mut self.current_batch)),
                Err(TrySendError::Full(())) => {} // no worries if not, keep batching while waiting for capacity
                Err(TrySendError::Closed(())) => anyhow::bail!("batch channel closed"),
            }
        }
        Ok(())
    }

    fn handle_set_record(
        &mut self,
        new: bool,
        commit: CommitData<serde_json::Value>,
        info: EventInfo,
    ) -> bool {
        let record = SetRecord {
            new,
            did: info.did,
            rkey: commit.info.rkey,
            record: commit.record,
        };
        let mut created_collection = false;
        let collection = self
            .current_batch
            .records
            .entry(commit.info.collection)
            .or_insert_with(|| {
                created_collection = true;
                Default::default()
            });
        collection.push_front(record);
        collection.truncate(MAX_BATCHED_RECORDS);

        if created_collection {
            self.current_batch.records.len() >= MAX_BATCHED_COLLECTIONS // full if we have collections to the max
        } else {
            false
        }
    }

    fn handle_delete_record(&mut self, commit_info: CommitInfo, info: EventInfo) -> bool {
        let rm = DeleteRecord {
            did: info.did,
            collection: commit_info.collection,
            rkey: commit_info.rkey,
        };
        self.current_batch.record_deletes.push(rm);
        self.current_batch.record_deletes.len() >= MAX_BATCHED_DELETES
    }

    fn handle_remove_account(&mut self, did: Did) -> bool {
        self.current_batch.account_removes.push(did);
        self.current_batch.account_removes.len() >= MAX_ACCOUNT_REMOVES
    }
}

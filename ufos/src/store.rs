use crate::{CreateRecord, EventBatch, Nsid};
use fjall::{BlockCache, Config, Keyspace, PartitionCreateOptions, PartitionHandle, Slice};
use jetstream::events::Cursor;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::{sync::mpsc::Receiver, time::sleep};

/**
 * data format, roughly:
 *
 * Global Meta:
 *   ["js_cursor"] => js_cursor(u64), // used as global sequence
 *   ["js_endpoint"] => &str, // checked on startup because jetstream instance cursors are not interchangeable
 *   ["mod_cursor"] => [u64];
 *   ["rollup_cursor"] => [js_cursor|collection]; // how far the rollup helper has progressed
 * Mod queue
 *   ["mod_queue"|mod_cursor] => one of {
 *      DeleteAccount(did, js_cursor) // delete all account content older than cursor
 *      DeleteRecord(did, collection, rkey, js_cursor) // delete record older than cursor
 *      UpdateRecord(did, collection, rkey, new_record, js_cursor) // delete + put, but don't delete if cursor is newer
 *   }
 * Collection and rollup meta:
 *   ["seen_by_js_cursor_collection"|js_cursor|collection] => u64 // batched total, gets cleaned up by rollup
 *   ["total_by_collection"|collection] => [u64, js_cursor] // live total requires scanning seen_by_collection after js_cursor
 *   ["hour_by_collection"|hour(u64)|collection] => u64 // rollup: computed by helper task based on dirty collections
 * Samples:
 *   ["by_collection"|collection|js_cursor] => [did|rkey|record]
 *   ["by_id"|did|collection|rkey|js_cursor] => [] // required to support deletes; did first prefix for account deletes.
 *
 * TODO: account privacy preferences. Might wait for the protocol-level (PDS-level?) stuff to land. Will probably do lazy
 * fetching + caching on read.
 **/
pub struct Storage {
    keyspace: Keyspace,
    partition: PartitionHandle,
}

impl Storage {
    pub fn open(path: impl AsRef<Path>, endpoint: &str) -> anyhow::Result<(Self, Option<Cursor>)> {
        // TODO: make this async? or should the caller remember that storage is sync?
        let keyspace = Config::new(path)
            .block_cache(Arc::new(BlockCache::with_capacity_bytes(
                128 * 2_u64.pow(20),
            )))
            .fsync_ms(Some(1000))
            .open()?;

        let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;

        let js_cursor = partition.get("js_cursor")?.map(cursor_from_slice);

        if js_cursor.is_some() {
            let Some(endpoint_bytes) = partition.get("js_endpoint")? else {
                anyhow::bail!("found cursor but missing js_endpoint, refusing to start.");
            };
            let stored = std::str::from_utf8(endpoint_bytes.as_ref())?;
            if stored != endpoint {
                anyhow::bail!("stored js_endpoint {stored:?} differs from provided {endpoint:?}, refusing to start.");
            }
        } else {
            partition.insert("js_endpoint", endpoint.as_bytes())?;
        }

        Ok((
            Self {
                keyspace,
                partition,
            },
            js_cursor,
        ))
    }

    pub async fn receive(&self, mut receiver: Receiver<EventBatch>) -> anyhow::Result<()> {
        loop {
            sleep(Duration::from_secs_f64(0.5)).await; // TODO: minimize during replay
            if let Some(event_batch) = receiver.recv().await {
                let last = event_batch.last_jetstream_cursor.clone(); // TODO: get this from the data. track last in consumer. compute or track first.
                let mut db_batch = self.keyspace.batch();

                for (collection, records) in &event_batch.record_creates {
                    for record in &records.samples {
                        // ["by_collection"|collection|js_cursor] => [did|rkey|record]
                        // ["by_id"|did|collection|rkey|js_cursor] => [] // required to support deletes; did first prefix for account deletes.

                        let key = by_collection_key_to_bytes(collection, record);
                        let value = by_collection_value_to_bytes(record);
                        db_batch.insert(&self.partition, key, value);
                    }
                }

                summarize(event_batch);
                if let Some(cursor) = last {
                    db_batch.insert(&self.partition, "js_cursor", cursor_to_slice(cursor));
                }
                db_batch.commit()?;
            } else {
                anyhow::bail!("receive channel closed");
            }
        }
    }
}

fn summarize(batch: EventBatch) {
    let EventBatch {
        record_creates,
        record_modifies,
        account_removes,
        last_jetstream_cursor,
        ..
    } = batch;
    let total_records: usize = record_creates.values().map(|v| v.total_seen).sum();
    let total_samples: usize = record_creates.values().map(|v| v.samples.len()).sum();
    println!(
        "got batch of {total_samples: >3} samples from {total_records: >3} records in {: >2} collections, {: >2} record modifies, {} account removes, cursor {:?}",
        record_creates.len(),
        record_modifies.len(),
        account_removes.len(),
        last_jetstream_cursor.map(|c| c.elapsed())
    );
}

fn cursor_to_slice(cursor: Cursor) -> [u8; 8] {
    cursor.to_raw_u64().to_be_bytes()
}
fn cursor_from_slice(bytes: Slice) -> Cursor {
    let mut buf = [0u8; 8];
    let len = 8.min(bytes.len());
    buf[..len].copy_from_slice(&bytes[..len]);
    Cursor::from_raw_u64(u64::from_be_bytes(buf))
}

fn by_collection_key_to_bytes(collection: &Nsid, record: &CreateRecord) -> Vec<u8> {
    vec![]
}
fn by_collection_value_to_bytes(record: &CreateRecord) -> Vec<u8> {
    vec![]
}

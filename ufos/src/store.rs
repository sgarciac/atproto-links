use crate::EventBatch;
use fjall::{BlockCache, Config, PartitionCreateOptions, Slice, TxKeyspace};
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
    keyspace: TxKeyspace,
}

impl Storage {
    pub fn open(path: impl AsRef<Path>, endpoint: &str) -> anyhow::Result<(Self, Option<Cursor>)> {
        // TODO: make this async? or should the caller remember that storage is sync?
        let keyspace = Config::new(path)
            .block_cache(Arc::new(BlockCache::with_capacity_bytes(
                128 * 2_u64.pow(20),
            )))
            .fsync_ms(Some(1000))
            .open_transactional()?;

        let meta = keyspace.open_partition("meta", PartitionCreateOptions::default())?;

        let js_cursor = meta
            .get("js_cursor")?
            .map(|c| Cursor::from_raw_u64(to_u64(c)));

        if js_cursor.is_some() {
            let Some(endpoint_bytes) = meta.get("js_endpoint")? else {
                anyhow::bail!("found cursor but missing js_endpoint, refusing to start.");
            };
            let stored = std::str::from_utf8(endpoint_bytes.as_ref())?;
            if stored != endpoint {
                anyhow::bail!("stored js_endpoint {stored:?} differs from provided {endpoint:?}, refusing to start.");
            }
        } else {
            let mut tx = keyspace.write_tx();
            tx.insert(&meta, "js_endpoint", endpoint.as_bytes());
            tx.commit()?;
        }

        Ok((Self { keyspace }, js_cursor))
    }

    pub async fn receive(&self, mut receiver: Receiver<EventBatch>) -> anyhow::Result<()> {
        let meta = self
            .keyspace
            .open_partition("meta", PartitionCreateOptions::default())?;
        loop {
            sleep(Duration::from_secs_f64(0.5)).await;
            if let Some(batch) = receiver.recv().await {
                let c = batch.last_jetstream_cursor.clone();
                summarize(batch);
                if let Some(cursor) = c {
                    let mut tx = self.keyspace.write_tx();
                    tx.insert(&meta, "js_cursor", from_u64(cursor.to_raw_u64()));
                    tx.commit()?;
                }
            } else {
                anyhow::bail!("receive channel closed")
            }
        }
    }
}

fn summarize(batch: EventBatch) {
    let EventBatch {
        records,
        record_deletes,
        account_removes,
        last_jetstream_cursor,
    } = batch;
    let total_records: usize = records.values().map(|v| v.total_seen).sum();
    let total_samples: usize = records.values().map(|v| v.samples.len()).sum();
    println!(
        "got batch of {total_samples: >3} samples from {total_records: >3} records in {: >2} collections, {: >2} record deletes, {} account removes, cursor {last_jetstream_cursor:?}",
        records.len(),
        record_deletes.len(),
        account_removes.len()
    );
}

fn from_u64(u: u64) -> [u8; 8] {
    u.to_be_bytes()
}
fn to_u64(bytes: Slice) -> u64 {
    let mut buf = [0u8; 8];
    let len = 8.min(bytes.len());
    buf[..len].copy_from_slice(&bytes[..len]);
    u64::from_be_bytes(buf)
}

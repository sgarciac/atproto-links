use crate::db_types::{db_complete, DbBytes, DbStaticStr, EncodingError, StaticStr};
use crate::store_types::{
    ByCollectionKey, ByCollectionValue, ByCursorSeenKey, ByCursorSeenValue, ByIdKey, ByIdValue,
    JetstreamCursorKey, JetstreamCursorValue, JetstreamEndpointKey, JetstreamEndpointValue,
    ModCursorKey, ModCursorValue, ModQueueItemKey, ModQueueItemPrefix, ModQueueItemStringValue,
    ModQueueItemValue,
};
use crate::{
    CollectionSamples, CreateRecord, DeleteAccount, Did, EventBatch, ModifyRecord, Nsid, RecordKey,
};
use fjall::{
    Batch as FjallBatch, CompressionType, Config, Keyspace, PartitionCreateOptions, PartitionHandle,
};
use jetstream::events::Cursor;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::{sync::mpsc::Receiver, time::sleep};

/**
 * data format, roughly:
 *
 * Global Meta:
 *   ["js_cursor"] => js_cursor(u64), // used as global sequence
 *   ["js_endpoint"] => &str, // checked on startup because jetstream instance cursors are not interchangeable
 *   ["mod_cursor"] => js_cursor(u64);
 *   ["rollup_cursor"] => [js_cursor|collection]; // how far the rollup helper has progressed
 * Mod queue
 *   ["mod_queue"|js_cursor] => one of {
 *      DeleteAccount(did) // delete all account content older than cursor
 *      DeleteRecord(did, collection, rkey) // delete record older than cursor
 *      UpdateRecord(did, collection, rkey, new_record) // delete + put, but don't delete if cursor is newer
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
#[derive(Clone)]
pub struct Storage {
    keyspace: Keyspace,
    partition: PartitionHandle,
}

impl Storage {
    fn init_self(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let keyspace = Config::new(path).fsync_ms(Some(4_000)).open()?;
        let partition = keyspace.open_partition(
            "default",
            PartitionCreateOptions::default().compression(CompressionType::None),
        )?;
        Ok(Self {
            keyspace,
            partition,
        })
    }

    pub async fn open(
        path: PathBuf,
        endpoint: &str,
        force_endpoint: bool,
    ) -> anyhow::Result<(Self, Option<Cursor>)> {
        let me = tokio::task::spawn_blocking(move || Storage::init_self(path)).await??;

        let js_cursor = me.get_jetstream_cursor().await?;

        if js_cursor.is_some() {
            let Some(JetstreamEndpointValue(stored)) = me.get_jetstream_endpoint().await? else {
                anyhow::bail!("found cursor but missing js_endpoint, refusing to start.");
            };
            if stored != endpoint {
                if force_endpoint {
                    log::warn!("forcing a jetstream switch from {stored:?} to {endpoint:?}");
                    me.set_jetstream_endpoint(endpoint).await?;
                } else {
                    anyhow::bail!("stored js_endpoint {stored:?} differs from provided {endpoint:?}, refusing to start.");
                }
            }
        } else {
            me.set_jetstream_endpoint(endpoint).await?;
        }

        Ok((me, js_cursor))
    }

    /// Jetstream event batch receiver: writes without any reads
    ///
    /// Events that require reads like record updates or delets are written to a queue
    pub async fn receive(&self, mut receiver: Receiver<EventBatch>) -> anyhow::Result<()> {
        // TODO: see rw_loop: enforce single-thread.
        loop {
            let t_sleep = Instant::now();
            sleep(Duration::from_secs_f64(0.3)).await; // TODO: minimize during replay
            let slept_for = t_sleep.elapsed();
            let queue_size = receiver.len();

            if let Some(event_batch) = receiver.recv().await {
                let batch_summary = summarize_batch(&event_batch);

                let last = event_batch.last_jetstream_cursor.clone(); // TODO: get this from the data. track last in consumer. compute or track first.

                let keyspace = self.keyspace.clone();
                let partition = self.partition.clone();

                let writer_t0 = Instant::now();
                tokio::task::spawn_blocking(move || {
                    DBWriter {
                        keyspace,
                        partition,
                    }
                    .write_batch(event_batch, last)
                })
                .await??;
                let wrote_for = writer_t0.elapsed();

                println!("{batch_summary}, slept {slept_for: <12?}, wrote {wrote_for: <11?}, queue: {queue_size}");
            } else {
                anyhow::bail!("receive channel closed");
            }
        }
    }

    /// Read-write loop reads from the queue for record-modifying events and does rollups
    pub async fn rw_loop(&self) -> anyhow::Result<()> {
        // TODO: lock so that only one rw loop can possibly be run. or even better, take a mutable resource thing to enforce at compile time.
        loop {
            sleep(Duration::from_secs_f64(0.2)).await;
            let keyspace = self.keyspace.clone();
            let partition = self.partition.clone();
            tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
                let prefix = ModQueueItemPrefix::default().to_db_bytes()?;
                // TODO: use the mod cursor to avoid scanning over all the deletes (delete range would actually be nice)
                let Some(pair) = partition.prefix(prefix).next() else {
                    eprintln!("mod queue empty.");
                    return Ok(());
                };
                let (key_bytes, val_bytes) = pair?;
                let mod_key: ModQueueItemKey = db_complete::<ModQueueItemKey>(&key_bytes)?;
                let mod_value: ModQueueItemValue =
                    db_complete::<ModQueueItemStringValue>(&val_bytes)?.try_into()?;

                DBWriter {
                    keyspace,
                    partition,
                }
                .write_rw(mod_key, mod_value)?;
                Ok(())
            })
            .await??;
        }
    }

    pub async fn get_collection_records(
        &self,
        collection: &Nsid,
        limit: usize,
    ) -> anyhow::Result<Vec<CreateRecord>> {
        let partition = self.partition.clone();
        let prefix = ByCollectionKey::prefix_from_collection(collection.clone())?;
        tokio::task::spawn_blocking(move || {
            let mut output = Vec::new();
            for pair in partition.prefix(&prefix).rev().take(limit) {
                let (k_bytes, v_bytes) = pair?;
                let (_, cursor) = db_complete::<ByCollectionKey>(&k_bytes)?.into();
                let (did, rkey, record) = db_complete::<ByCollectionValue>(&v_bytes)?.into();
                output.push(CreateRecord {
                    did,
                    rkey,
                    record,
                    cursor,
                })
            }
            Ok(output)
        })
        .await?
    }

    pub async fn get_meta_info(&self) -> anyhow::Result<StorageInfo> {
        let keyspace = self.keyspace.clone();
        let partition = self.partition.clone();
        tokio::task::spawn_blocking(move || {
            Ok(StorageInfo {
                keyspace_disk_space: keyspace.disk_space(),
                keyspace_journal_count: keyspace.journal_count(),
                keyspace_sequence: keyspace.instant(),
                partition_approximate_len: partition.approximate_len(),
            })
        })
        .await?
    }

    pub async fn get_jetstream_endpoint(&self) -> anyhow::Result<Option<JetstreamEndpointValue>> {
        let partition = self.partition.clone();
        tokio::task::spawn_blocking(move || {
            get_static::<JetstreamEndpointKey, JetstreamEndpointValue>(&partition)
        })
        .await?
    }

    async fn set_jetstream_endpoint(&self, endpoint: &str) -> anyhow::Result<()> {
        let partition = self.partition.clone();
        let endpoint = endpoint.to_string();
        tokio::task::spawn_blocking(move || {
            insert_static::<JetstreamEndpointKey>(&partition, JetstreamEndpointValue(endpoint))
        })
        .await?
    }

    pub async fn get_jetstream_cursor(&self) -> anyhow::Result<Option<Cursor>> {
        let partition = self.partition.clone();
        tokio::task::spawn_blocking(move || {
            get_static::<JetstreamCursorKey, JetstreamCursorValue>(&partition)
        })
        .await?
    }

    pub async fn get_mod_cursor(&self) -> anyhow::Result<Option<Cursor>> {
        let partition = self.partition.clone();
        tokio::task::spawn_blocking(move || get_static::<ModCursorKey, ModCursorValue>(&partition))
            .await?
    }
}

/// Get a value from a fixed key
fn get_static<K: StaticStr, V: DbBytes>(partition: &PartitionHandle) -> anyhow::Result<Option<V>> {
    let key_bytes = DbStaticStr::<K>::default().to_db_bytes()?;
    let value = partition
        .get(&key_bytes)?
        .map(|value_bytes| db_complete(&value_bytes))
        .transpose()?;
    Ok(value)
}

/// Set a value to a fixed key
fn insert_static<K: StaticStr>(
    partition: &PartitionHandle,
    value: impl DbBytes,
) -> anyhow::Result<()> {
    let key_bytes = DbStaticStr::<K>::default().to_db_bytes()?;
    let value_bytes = value.to_db_bytes()?;
    partition.insert(&key_bytes, &value_bytes)?;
    Ok(())
}

/// Set a value to a fixed key
fn insert_batch_static<K: StaticStr>(
    batch: &mut FjallBatch,
    partition: &PartitionHandle,
    value: impl DbBytes,
) -> anyhow::Result<()> {
    let key_bytes = DbStaticStr::<K>::default().to_db_bytes()?;
    let value_bytes = value.to_db_bytes()?;
    batch.insert(partition, &key_bytes, &value_bytes);
    Ok(())
}

/// Remove a key
fn remove_batch<K: DbBytes>(
    batch: &mut FjallBatch,
    partition: &PartitionHandle,
    key: K,
) -> Result<(), EncodingError> {
    let key_bytes = key.to_db_bytes()?;
    batch.remove(partition, &key_bytes);
    Ok(())
}

impl DBWriter {
    fn write_batch(self, event_batch: EventBatch, last: Option<Cursor>) -> anyhow::Result<()> {
        let mut db_batch = self.keyspace.batch();
        self.add_record_creates(&mut db_batch, event_batch.record_creates)?;
        self.add_record_modifies(&mut db_batch, event_batch.record_modifies)?;
        self.add_account_removes(&mut db_batch, event_batch.account_removes)?;
        if let Some(cursor) = last {
            insert_batch_static::<JetstreamCursorKey>(&mut db_batch, &self.partition, cursor)?;
        }
        Ok(db_batch.commit()?)
    }

    fn write_rw(
        self,
        mod_key: ModQueueItemKey,
        mod_value: ModQueueItemValue,
    ) -> anyhow::Result<()> {
        let mut db_batch = self.keyspace.batch();

        // update the current rw cursor to this item (atomically with the batch if it succeeds)
        let mod_cursor_value: ModCursorValue = (&mod_key).into();
        insert_batch_static::<ModCursorKey>(
            &mut db_batch,
            &self.partition,
            mod_cursor_value.clone(),
        )?;

        // remove the queued rw task so that we'll continue with the *next* one (atomically with batch)
        remove_batch::<ModQueueItemKey>(&mut db_batch, &self.partition, mod_key)?;

        match mod_value {
            ModQueueItemValue::DeleteAccount(did) => {
                eprintln!("delete account: {did:?} (not yet implemented)");
                return Ok(()); // don't let the batch commit until we implement this
            }
            ModQueueItemValue::DeleteRecord(did, collection, rkey) => {
                eprintln!("delete record: {did:?} {collection:?} {rkey:?}");
                self.delete_record(&mut db_batch, mod_cursor_value, did, collection, rkey)?;
            }
            ModQueueItemValue::UpdateRecord(did, collection, rkey, record) => {
                eprintln!("update record: {did:?} {collection:?} {rkey:?} {record:?} (not yet implemented)");
                return Ok(()); // don't let the batch commit until we implement this
            }
        }
        Ok(db_batch.commit()?)
    }

    fn delete_record(
        &self,
        db_batch: &mut FjallBatch,
        cursor: Cursor,
        did: Did,
        collection: Nsid,
        rkey: RecordKey,
    ) -> anyhow::Result<()> {
        let key_prefix_bytes =
            ByIdKey::record_prefix(did, collection.clone(), rkey).to_db_bytes()?;

        let mut n_removed = 0;
        for pair in self.partition.prefix(&key_prefix_bytes) {
            // find all (hopefully 1)
            let (key_bytes, _) = pair?;
            let found_cursor = db_complete::<ByIdKey>(&key_bytes)?.cursor();
            if found_cursor > cursor {
                // we are *only* allowed to delete records that came before the record delete event
                eprintln!("delete_record: found and ignoring newer version(s)");
                break;
            }

            // remove the by_id entry
            db_batch.remove(&self.partition, key_bytes);

            // remove its record sample
            let by_collection_key_bytes =
                ByCollectionKey::new(collection.clone(), found_cursor).to_db_bytes()?;
            db_batch.remove(&self.partition, by_collection_key_bytes);

            n_removed += 1;
        }

        eprintln!("removed {n_removed} records.");
        Ok(())
    }

    fn add_record_creates(
        &self,
        db_batch: &mut FjallBatch,
        record_creates: HashMap<Nsid, CollectionSamples>,
    ) -> anyhow::Result<()> {
        for (
            collection,
            CollectionSamples {
                total_seen,
                samples,
            },
        ) in record_creates.into_iter()
        {
            if let Some(last_record) = &samples.back() {
                db_batch.insert(
                    &self.partition,
                    ByCursorSeenKey::new(last_record.cursor.clone(), collection.clone())
                        .to_db_bytes()?,
                    ByCursorSeenValue::new(total_seen as u64).to_db_bytes()?,
                );
            } else {
                log::error!(
                    "collection samples should only exist when at least one sample has been added"
                );
            }

            for CreateRecord {
                did,
                rkey,
                cursor,
                record,
            } in samples.into_iter().rev()
            {
                // ["by_collection"|collection|js_cursor] => [did|rkey|record]
                db_batch.insert(
                    &self.partition,
                    ByCollectionKey::new(collection.clone(), cursor.clone()).to_db_bytes()?,
                    ByCollectionValue::new(did.clone(), rkey.clone(), record).to_db_bytes()?,
                );

                // ["by_id"|did|collection|rkey|js_cursor] => [] // required to support deletes; did first prefix for account deletes.
                db_batch.insert(
                    &self.partition,
                    ByIdKey::new(did, collection.clone(), rkey, cursor).to_db_bytes()?,
                    ByIdValue::default().to_db_bytes()?,
                );
            }
        }
        Ok(())
    }

    fn add_record_modifies(
        &self,
        db_batch: &mut FjallBatch,
        record_modifies: Vec<ModifyRecord>,
    ) -> anyhow::Result<()> {
        for modification in record_modifies {
            let (cursor, db_val) = match modification {
                ModifyRecord::Update(u) => (
                    u.cursor,
                    ModQueueItemValue::UpdateRecord(u.did, u.collection, u.rkey, u.record),
                ),
                ModifyRecord::Delete(d) => (
                    d.cursor,
                    ModQueueItemValue::DeleteRecord(d.did, d.collection, d.rkey),
                ),
            };
            db_batch.insert(
                &self.partition,
                ModQueueItemKey::new(cursor).to_db_bytes()?,
                db_val.to_db_bytes()?,
            );
        }
        Ok(())
    }

    fn add_account_removes(
        &self,
        db_batch: &mut FjallBatch,
        account_removes: Vec<DeleteAccount>,
    ) -> anyhow::Result<()> {
        for deletion in account_removes {
            db_batch.insert(
                &self.partition,
                ModQueueItemKey::new(deletion.cursor).to_db_bytes()?,
                ModQueueItemValue::DeleteAccount(deletion.did).to_db_bytes()?,
            );
        }
        Ok(())
    }
}

#[derive(Debug, serde::Serialize, schemars::JsonSchema)]
pub struct StorageInfo {
    pub keyspace_disk_space: u64,
    pub keyspace_journal_count: usize,
    pub keyspace_sequence: u64,
    pub partition_approximate_len: usize,
}

struct DBWriter {
    keyspace: Keyspace,
    partition: PartitionHandle,
}

////////// temp stuff to remove:

fn summarize_batch(batch: &EventBatch) -> String {
    let EventBatch {
        record_creates,
        record_modifies,
        account_removes,
        last_jetstream_cursor,
        ..
    } = batch;
    let total_records: usize = record_creates.values().map(|v| v.total_seen).sum();
    let total_samples: usize = record_creates.values().map(|v| v.samples.len()).sum();
    format!(
        "batch of {total_samples: >3} samples from {total_records: >4} records in {: >2} collections, {: >3} modifies, {} acct removes, cursor {: <12?}",
        record_creates.len(),
        record_modifies.len(),
        account_removes.len(),
        last_jetstream_cursor.clone().map(|c| c.elapsed())
    )
}

use crate::db_types::{db_complete, DbBytes, DbStaticStr, EncodingError, StaticStr};
use crate::error::StorageError;
use crate::storage::{StorageWhatever, StoreReader, StoreWriter};
use crate::store_types::{
    ByCollectionKey, ByCollectionValue, ByCursorSeenKey, ByCursorSeenValue, ByIdKey, ByIdValue,
    DeleteAccountQueueKey, DeleteAccountQueueVal, JetstreamCursorKey, JetstreamCursorValue,
    JetstreamEndpointKey, JetstreamEndpointValue, LiveDidsKey, LiveDidsValue, LiveRecordsKey,
    LiveRecordsValue, ModCursorKey, ModCursorValue, ModQueueItemKey, ModQueueItemStringValue,
    ModQueueItemValue, NewRollupCursorKey, NewRollupCursorValue, NsidRecordFeedKey,
    NsidRecordFeedVal, RecordLocationKey, RecordLocationVal, RollupCursorKey, RollupCursorValue,
    SeenCounter, TakeoffKey, TakeoffValue,
};
use crate::{CommitAction, DeleteAccount, Did, EventBatch, Nsid, RecordKey};
use fjall::{
    Batch as FjallBatch, CompressionType, Config, Keyspace, PartitionCreateOptions, PartitionHandle,
};
use jetstream::events::Cursor;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::mpsc::Receiver;
use tokio::time::{interval_at, sleep};

/// Commit the RW batch immediately if this number of events have been read off the mod queue
const MAX_BATCHED_RW_EVENTS: usize = 18;

/// Commit the RW batch immediately if this number of records is reached
///
/// there are probably some efficiency gains for higher, at cost of more memory.
/// interestingly, this kind of sets a priority weight for the RW loop:
///     - doing more work whenever scheduled means getting more CPU time in general
///
/// this is higher than [MAX_BATCHED_RW_EVENTS] because account-deletes can have lots of items
const MAX_BATCHED_RW_ITEMS: usize = 24;

#[derive(Clone)]
struct Db {
    keyspace: Keyspace,
    global: PartitionHandle,
}

/**
 * new data format, roughly:
 *
 * Partion: 'global'
 *
 *  - Global sequence counter (is the jetstream cursor -- monotonic with many gaps)
 *      key: "js_cursor" (literal)
 *      val: u64
 *
 *  - Jetstream server endpoint (persisted because the cursor can't be used on another instance without data loss)
 *      key: "js_endpoint" (literal)
 *      val: string (URL of the instance)
 *
 *  - Launch date
 *      key: "takeoff" (literal)
 *      val: u64 (micros timestamp, not from jetstream for now so not precise)
 *
 *  - Rollup cursor (bg work: roll stats into hourlies, delete accounts, old record deletes)
 *      key: "rollup_cursor" (literal)
 *      val: u64 (tracks behind js_cursor)
 *
 *
 * Partition: 'feed'
 *
 *  - Per-collection list of record references ordered by jetstream cursor
 *      key: nullstr || u64 (collection nsid null-terminated, jetstream cursor)
 *      val: nullstr || nullstr || nullstr (did, rkey, rev. rev is mostly a sanity-check for now.)
 *
 *
 * Partition: 'records'
 *
 *  - Actual records by their atproto location
 *      key: nullstr || nullstr || nullstr (did, collection, rkey)
 *      val: u64 || bool || nullstr || rawval (js_cursor, is_update, rev, actual record)
 *
 * Partition: 'rollups'
 *
 * - Live (batched) records per collection
 *      key: "live_records" || u64 || nullstr (js_cursor, nsid)
 *      val: u64
 *
 * - Live (batched) DIDs estimate per collections
 *      key: "live_dids" || u64 || nullstr
 *      val: HLL (estimator)
 *
 * - Hourly total records per collection
 *      key: "hourly_records" || u64 || nullstr (hour, nsid)
 *      val: u64 (total count, not jetstream cursor)
 *
 * - Hourly unique DIDs estimate per collection
 *      key: "hourly_dids" || u64 || nullstr (hour, nsid)
 *      val: HLL (estimator)
 *
 * - All-time total records per collection
 *      key: "ever_records" || u64 || nullstr (total, nsid. yeah, total is in the *key*, and acts as a sorter. every update requires a delete+put)
 *      val: (empty)
 *
 * - All-time total DIDs estimate per collection
 *      key: "ever_dids" || u64 || nullstr (estimated cardinality, nsid. like ever_records)
 *      val: HLL (estimator)
 *
 *
 * Partition: 'queues'
 *
 *  - Delete account queue
 *      key: "delete_acount" || u64 (js_cursor)
 *      val: nullstr (did)
 *
 *
 * TODO: moderation actions
 * TODO: account privacy preferences. Might wait for the protocol-level (PDS-level?) stuff to land. Will probably do lazy fetching + caching on read.
 **/
#[derive(Debug)]
pub struct FjallStorage {}

#[derive(Debug, Default)]
pub struct FjallConfig {
    /// drop the db when the storage is dropped
    ///
    /// this is only meant for tests
    #[cfg(test)]
    pub temp: bool,
}

impl StorageWhatever<FjallReader, FjallWriter, FjallConfig> for FjallStorage {
    fn init(
        path: impl AsRef<Path>,
        endpoint: String,
        force_endpoint: bool,
        _config: FjallConfig,
    ) -> Result<(FjallReader, FjallWriter, Option<Cursor>), StorageError> {
        let keyspace = {
            let config = Config::new(path);

            #[cfg(not(test))]
            let config = config.fsync_ms(Some(4_000));

            config.open()?
        };

        let global = keyspace.open_partition("global", PartitionCreateOptions::default())?;
        let feeds = keyspace.open_partition("feeds", PartitionCreateOptions::default())?;
        let records = keyspace.open_partition("records", PartitionCreateOptions::default())?;
        let rollups = keyspace.open_partition("rollups", PartitionCreateOptions::default())?;
        let queues = keyspace.open_partition("queues", PartitionCreateOptions::default())?;

        let js_cursor = get_static_neu::<JetstreamCursorKey, JetstreamCursorValue>(&global)?;

        if js_cursor.is_some() {
            let stored_endpoint =
                get_static_neu::<JetstreamEndpointKey, JetstreamEndpointValue>(&global)?;

            let JetstreamEndpointValue(stored) = stored_endpoint.ok_or(StorageError::InitError(
                "found cursor but missing js_endpoint, refusing to start.".to_string(),
            ))?;

            if stored != endpoint {
                if force_endpoint {
                    log::warn!("forcing a jetstream switch from {stored:?} to {endpoint:?}");
                    insert_static_neu::<JetstreamEndpointKey>(
                        &global,
                        JetstreamEndpointValue(endpoint.to_string()),
                    )?;
                } else {
                    return Err(StorageError::InitError(format!(
                        "stored js_endpoint {stored:?} differs from provided {endpoint:?}, refusing to start.")));
                }
            }
        } else {
            insert_static_neu::<JetstreamEndpointKey>(
                &global,
                JetstreamEndpointValue(endpoint.to_string()),
            )?;
            insert_static_neu::<TakeoffKey>(&global, Cursor::at(SystemTime::now()))?;
            insert_static_neu::<NewRollupCursorKey>(&global, Cursor::from_start())?;
        }

        let reader = FjallReader {
            global: global.clone(),
            feeds: feeds.clone(),
            records: records.clone(),
            rollups: rollups.clone(),
        };
        let writer = FjallWriter {
            keyspace,
            global,
            feeds,
            records,
            rollups,
            queues,
        };
        Ok((reader, writer, js_cursor))
    }
}

#[derive(Clone)]
pub struct FjallReader {
    global: PartitionHandle,
    feeds: PartitionHandle,
    records: PartitionHandle,
    rollups: PartitionHandle,
}

impl StoreReader for FjallReader {
    fn get_total_by_collection(&self, collection: &Nsid) -> Result<u64, StorageError> {
        // TODO: start from rollup
        let full_range = LiveRecordsKey::range_from_cursor(Cursor::from_start())?;
        let mut total = 0;
        for kv in self.rollups.range(full_range) {
            let (key_bytes, val_bytes) = kv?;
            let key = db_complete::<LiveRecordsKey>(&key_bytes)?;
            if key.collection() == collection {
                let LiveRecordsValue(n) = db_complete(&val_bytes)?;
                total += n;
            }
        }
        Ok(total)
    }
    fn get_dids_by_collection(&self, collection: &Nsid) -> Result<u64, StorageError> {
        // TODO: start from rollup
        let full_range = LiveDidsKey::range_from_cursor(Cursor::from_start())?;
        let mut total_estimate = cardinality_estimator::CardinalityEstimator::new();
        for kv in self.rollups.range(full_range) {
            let (key_bytes, val_bytes) = kv?;
            let key = db_complete::<LiveDidsKey>(&key_bytes)?;
            if key.collection() == collection {
                let LiveDidsValue(estimate) = db_complete(&val_bytes)?;
                total_estimate.merge(&estimate);
            }
        }
        Ok(total_estimate.estimate() as u64)
    }
}

pub struct FjallWriter {
    keyspace: Keyspace,
    global: PartitionHandle,
    feeds: PartitionHandle,
    records: PartitionHandle,
    rollups: PartitionHandle,
    queues: PartitionHandle,
}

impl FjallWriter {
    pub fn step_rollup(&mut self) -> Result<(), StorageError> {
        // let mut batch = self.keyspace.batch();

        let rollup_cursor =
            get_static_neu::<NewRollupCursorKey, NewRollupCursorValue>(&self.global)?.ok_or(
                StorageError::BadStateError("Could not find current rollup cursor".to_string()),
            )?;

        // timelies
        let live_records_range = LiveRecordsKey::range_from_cursor(rollup_cursor)?;
        // let live_dids_range = LiveDidsKey::range_from_cursor(rollup_cursor)?; // shoudl be in sync with live records range. we could keep both value under same key?
        let mut timely_iter = self.rollups.range(live_records_range);

        let next_timely = timely_iter
            .next()
            .transpose()?
            .map(|(key_bytes, val_bytes)| {
                db_complete::<LiveRecordsKey>(&key_bytes).map(|k| (k, val_bytes))
            })
            .transpose()?;

        // delete accounts
        let delete_accounts_range =
            DeleteAccountQueueKey::new(rollup_cursor).range_to_prefix_end()?;

        let next_delete = self
            .queues
            .range(delete_accounts_range)
            .next()
            .transpose()?
            .map(|(key_bytes, val_bytes)| {
                db_complete::<DeleteAccountQueueKey>(&key_bytes).map(|k| (k.suffix, val_bytes))
            })
            .transpose()?;

        match (next_timely, next_delete) {
            (Some((k, timely_val_bytes)), Some((cursor, delete_val_bytes))) => {
                if k.cursor() < cursor {
                    eprintln!("rollup until delete cursor");
                } else {
                    eprintln!("delete then come back for rollups");
                }
            }
            (Some((k, timely_val_bytes)), None) => {
                eprintln!("do as much rollup as we want");
            }
            (None, Some((cursor, delete_val_bytes))) => {
                eprintln!("just delete an account");
            }
            (None, None) => {
                eprintln!("do nothing.");
            }
        }

        // batch.commit()?;
        Ok(())
    }
}

impl StoreWriter for FjallWriter {
    fn insert_batch(&mut self, event_batch: EventBatch) -> Result<(), StorageError> {
        if event_batch.is_empty() {
            return Ok(());
        }

        let mut batch = self.keyspace.batch();

        // would be nice not to have to iterate everything at once here
        let latest = event_batch.latest_cursor().unwrap();

        for (nsid, commits) in event_batch.commits_by_nsid {
            for commit in commits.commits {
                let location_key: RecordLocationKey = (&commit, &nsid).into();

                match commit.action {
                    CommitAction::Cut => {
                        batch.remove(&self.records, &location_key.to_db_bytes()?);
                    }
                    CommitAction::Put(put_action) => {
                        let feed_key = NsidRecordFeedKey::from_pair(nsid.clone(), commit.cursor);
                        let feed_val: NsidRecordFeedVal =
                            (&commit.did, &commit.rkey, commit.rev.as_str()).into();
                        batch.insert(
                            &self.feeds,
                            feed_key.to_db_bytes()?,
                            feed_val.to_db_bytes()?,
                        );

                        let location_val: RecordLocationVal =
                            (commit.cursor, commit.rev.as_str(), put_action).into();
                        batch.insert(
                            &self.records,
                            &location_key.to_db_bytes()?,
                            &location_val.to_db_bytes()?,
                        );
                    }
                }
            }
            let live_records_key: LiveRecordsKey = (latest, &nsid).into();
            let live_records_value = LiveRecordsValue(commits.total_seen as u64);
            batch.insert(
                &self.rollups,
                &live_records_key.to_db_bytes()?,
                &live_records_value.to_db_bytes()?,
            );

            let live_dids_key: LiveDidsKey = (latest, &nsid).into();
            let live_dids_value = LiveDidsValue(commits.dids_estimate);
            batch.insert(
                &self.rollups,
                &live_dids_key.to_db_bytes()?,
                &live_dids_value.to_db_bytes()?,
            );
        }

        for remove in event_batch.account_removes {
            let queue_key = DeleteAccountQueueKey::new(remove.cursor);
            let queue_val: DeleteAccountQueueVal = remove.did;
            batch.insert(
                &self.queues,
                &queue_key.to_db_bytes()?,
                &queue_val.to_db_bytes()?,
            );
        }

        batch.insert(
            &self.global,
            DbStaticStr::<JetstreamCursorKey>::default().to_db_bytes()?,
            latest.to_db_bytes()?,
        );

        batch.commit()?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct Storage {
    /// horrible: gate all db access behind this to force global serialization to avoid deadlock
    db: Db,
}

impl Storage {
    fn init_self(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let keyspace = Config::new(path).fsync_ms(Some(4_000)).open()?;
        let global = keyspace.open_partition(
            "default",
            PartitionCreateOptions::default().compression(CompressionType::None),
        )?;
        Ok(Self {
            db: Db { keyspace, global },
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
            sleep(Duration::from_secs_f64(0.08)).await; // TODO: minimize during replay
            let _slept_for = t_sleep.elapsed();
            let _queue_size = receiver.len();

            if let Some(event_batch) = receiver.recv().await {
                log::info!("write: received write batch");
                let batch_summary = summarize_batch(&event_batch);
                log::info!("{}", batch_summary);

                // todo!();
                // let last = event_batch.last_jetstream_cursor.clone(); // TODO: get this from the data. track last in consumer. compute or track first.

                // let db = &self.db;
                // let keyspace = db.keyspace.clone();
                // let global = db.global.clone();

                // let writer_t0 = Instant::now();
                // log::trace!("spawn_blocking for write batch");
                // tokio::task::spawn_blocking(move || {
                //     DBWriter {
                //         keyspace,
                //         global,
                //     }
                //     .write_batch(event_batch, last)
                // })
                // .await??;
                // log::trace!("write: back from blocking task, successfully wrote batch");
                // let wrote_for = writer_t0.elapsed();

                // println!("{batch_summary}, slept {slept_for: <12?}, wrote {wrote_for: <11?}, queue: {queue_size}");
            } else {
                log::error!("store consumer: receive channel failed (dropped/closed?)");
                anyhow::bail!("receive channel closed");
            }
        }
    }

    /// Read-write loop reads from the queue for record-modifying events and does rollups
    pub async fn rw_loop(&self) -> anyhow::Result<()> {
        // TODO: lock so that only one rw loop can possibly be run. or even better, take a mutable resource thing to enforce at compile time.

        let now = tokio::time::Instant::now();
        let mut time_to_update_events = interval_at(now, Duration::from_secs_f64(0.051));
        let mut time_to_trim_surplus = interval_at(
            now + Duration::from_secs_f64(1.0),
            Duration::from_secs_f64(3.3),
        );
        let mut time_to_roll_up = interval_at(
            now + Duration::from_secs_f64(0.4),
            Duration::from_secs_f64(0.9),
        );

        time_to_update_events.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        time_to_trim_surplus.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        time_to_roll_up.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            let keyspace = self.db.keyspace.clone();
            let global = self.db.global.clone();
            tokio::select! {
                _ = time_to_update_events.tick() => {
                    log::debug!("beginning event update task");
                    tokio::task::spawn_blocking(move || Self::update_events(keyspace, global)).await??;
                    log::debug!("finished event update task");
                }
                _ = time_to_trim_surplus.tick() => {
                    log::debug!("beginning record trim task");
                    tokio::task::spawn_blocking(move || Self::trim_old_events(keyspace, global)).await??;
                    log::debug!("finished record trim task");
                }
                _ = time_to_roll_up.tick() => {
                    log::debug!("beginning rollup task");
                    tokio::task::spawn_blocking(move || Self::roll_up_counts(keyspace, global)).await??;
                    log::debug!("finished rollup task");
                },
            }
        }
    }

    fn update_events(keyspace: Keyspace, global: PartitionHandle) -> anyhow::Result<()> {
        // TODO: lock this to prevent concurrent rw

        log::trace!("rw: getting rw cursor...");
        let mod_cursor =
            get_static::<ModCursorKey, ModCursorValue>(&global)?.unwrap_or(Cursor::from_start());
        let range = ModQueueItemKey::new(mod_cursor.clone()).range_to_prefix_end()?;

        let mut db_batch = keyspace.batch();
        let mut batched_rw_items = 0;
        let mut any_tasks_found = false;

        log::trace!("rw: iterating newer rw items...");

        for (i, pair) in global.range(range.clone()).enumerate() {
            log::trace!("rw: iterating {i}");
            any_tasks_found = true;

            if i >= MAX_BATCHED_RW_EVENTS {
                break;
            }

            let (key_bytes, val_bytes) = pair?;
            let mod_key = match db_complete::<ModQueueItemKey>(&key_bytes) {
                Ok(k) => k,
                Err(EncodingError::WrongStaticPrefix(_, _)) => {
                    panic!("wsp: mod queue empty.");
                }
                otherwise => otherwise?,
            };

            let mod_value: ModQueueItemValue =
                db_complete::<ModQueueItemStringValue>(&val_bytes)?.try_into()?;

            log::trace!("rw: iterating {i}: sending to batcher {mod_key:?} => {mod_value:?}");
            batched_rw_items += DBWriter {
                keyspace: keyspace.clone(),
                global: global.clone(),
            }
            .write_rw(&mut db_batch, mod_key, mod_value)?;
            log::trace!("rw: iterating {i}: back from batcher.");

            if batched_rw_items >= MAX_BATCHED_RW_ITEMS {
                log::trace!("rw: iterating {i}: batch big enough, breaking out.");
                break;
            }
        }

        if !any_tasks_found {
            log::trace!("rw: skipping batch commit since apparently no items were added (this is normal, skipping is new)");
            // TODO: is this missing a chance to update the cursor?
            return Ok(());
        }

        log::info!("rw: committing rw batch with {batched_rw_items} items (items != total inserts/deletes)...");
        let r = db_batch.commit();
        log::info!("rw: commit result: {r:?}");
        r?;
        Ok(())
    }

    fn trim_old_events(_keyspace: Keyspace, _global: PartitionHandle) -> anyhow::Result<()> {
        // we *could* keep a collection dirty list in memory to reduce the amount of searching here
        // actually can we use seen_by_js_cursor_collection??
        // *   ["seen_by_js_cursor_collection"|js_cursor|collection] => u64
        // -> the rollup cursor could handle trims.

        // key structure:
        // *   ["by_collection"|collection|js_cursor] => [did|rkey|record]

        // *new* strategy:
        // 1. collect `collection`s seen during rollup
        // 2. for each collected collection:
        // 3. set up prefix iterator
        // 4. reverse and try to walk back MAX_RETAINED steps
        // 5. if we didn't end iteration yet, start deleting records (and their forward links) until we get to the end

        // oh we might be able to walk *forward* instead of reverse from the cursor, which might help avoid iterating over a lot of deletion tombstones

        // ... we can probably do even better with cursor ranges too, since we'll have a cursor range from rollup and it's in the by_collection key

        Ok(())
    }

    fn roll_up_counts(_keyspace: Keyspace, _global: PartitionHandle) -> anyhow::Result<()> {
        Ok(())
    }

    pub async fn get_collection_records(
        &self,
        _collection: &Nsid,
        _limit: usize,
    ) -> anyhow::Result<Vec<()>> {
        todo!();
        // let global = self.db.global.clone();
        // let prefix = ByCollectionKey::prefix_from_collection(collection.clone())?;
        // tokio::task::spawn_blocking(move || {
        //     let mut output = Vec::new();

        //     for pair in global.prefix(&prefix).rev().take(limit) {
        //         let (k_bytes, v_bytes) = pair?;
        //         let (_, cursor) = db_complete::<ByCollectionKey>(&k_bytes)?.into();
        //         let (did, rkey, record) = db_complete::<ByCollectionValue>(&v_bytes)?.into();
        //         output.push(CreateRecord {
        //             did,
        //             rkey,
        //             record,
        //             cursor,
        //         })
        //     }
        //     Ok(output)
        // })
        // .await?
    }

    pub async fn get_meta_info(&self) -> anyhow::Result<StorageInfo> {
        let db = &self.db;
        let keyspace = db.keyspace.clone();
        let global = db.global.clone();
        tokio::task::spawn_blocking(move || {
            Ok(StorageInfo {
                keyspace_disk_space: keyspace.disk_space(),
                keyspace_journal_count: keyspace.journal_count(),
                keyspace_sequence: keyspace.instant(),
                global_approximate_len: global.approximate_len(),
            })
        })
        .await?
    }

    pub async fn get_collection_total_seen(&self, collection: &Nsid) -> anyhow::Result<u64> {
        let global = self.db.global.clone();
        let collection = collection.clone();
        tokio::task::spawn_blocking(move || get_unrolled_collection_seen(&global, collection))
            .await?
    }

    pub async fn get_top_collections(&self) -> anyhow::Result<HashMap<String, u64>> {
        let global = self.db.global.clone();
        tokio::task::spawn_blocking(move || get_unrolled_top_collections(&global)).await?
    }

    pub async fn get_jetstream_endpoint(&self) -> anyhow::Result<Option<JetstreamEndpointValue>> {
        let global = self.db.global.clone();
        tokio::task::spawn_blocking(move || {
            get_static::<JetstreamEndpointKey, JetstreamEndpointValue>(&global)
        })
        .await?
    }

    async fn set_jetstream_endpoint(&self, endpoint: &str) -> anyhow::Result<()> {
        let global = self.db.global.clone();
        let endpoint = endpoint.to_string();
        tokio::task::spawn_blocking(move || {
            insert_static::<JetstreamEndpointKey>(&global, JetstreamEndpointValue(endpoint))
        })
        .await?
    }

    pub async fn get_jetstream_cursor(&self) -> anyhow::Result<Option<Cursor>> {
        let global = self.db.global.clone();
        tokio::task::spawn_blocking(move || {
            get_static::<JetstreamCursorKey, JetstreamCursorValue>(&global)
        })
        .await?
    }

    pub async fn get_mod_cursor(&self) -> anyhow::Result<Option<Cursor>> {
        let global = self.db.global.clone();
        tokio::task::spawn_blocking(move || get_static::<ModCursorKey, ModCursorValue>(&global))
            .await?
    }
}

/// Get a value from a fixed key
fn get_static<K: StaticStr, V: DbBytes>(global: &PartitionHandle) -> anyhow::Result<Option<V>> {
    let key_bytes = DbStaticStr::<K>::default().to_db_bytes()?;
    let value = global
        .get(&key_bytes)?
        .map(|value_bytes| db_complete(&value_bytes))
        .transpose()?;
    Ok(value)
}

/// Get a value from a fixed key
fn get_static_neu<K: StaticStr, V: DbBytes>(
    global: &PartitionHandle,
) -> Result<Option<V>, StorageError> {
    let key_bytes = DbStaticStr::<K>::default().to_db_bytes()?;
    let value = global
        .get(&key_bytes)?
        .map(|value_bytes| db_complete(&value_bytes))
        .transpose()?;
    Ok(value)
}

/// Set a value to a fixed key
fn insert_static<K: StaticStr>(
    global: &PartitionHandle,
    value: impl DbBytes,
) -> anyhow::Result<()> {
    let key_bytes = DbStaticStr::<K>::default().to_db_bytes()?;
    let value_bytes = value.to_db_bytes()?;
    global.insert(&key_bytes, &value_bytes)?;
    Ok(())
}

/// Set a value to a fixed key
fn insert_static_neu<K: StaticStr>(
    global: &PartitionHandle,
    value: impl DbBytes,
) -> Result<(), StorageError> {
    let key_bytes = DbStaticStr::<K>::default().to_db_bytes()?;
    let value_bytes = value.to_db_bytes()?;
    global.insert(&key_bytes, &value_bytes)?;
    Ok(())
}

/// Set a value to a fixed key
fn insert_batch_static<K: StaticStr>(
    batch: &mut FjallBatch,
    global: &PartitionHandle,
    value: impl DbBytes,
) -> anyhow::Result<()> {
    let key_bytes = DbStaticStr::<K>::default().to_db_bytes()?;
    let value_bytes = value.to_db_bytes()?;
    batch.insert(global, &key_bytes, &value_bytes);
    Ok(())
}

/// Remove a key
fn remove_batch<K: DbBytes>(
    batch: &mut FjallBatch,
    global: &PartitionHandle,
    key: K,
) -> Result<(), EncodingError> {
    let key_bytes = key.to_db_bytes()?;
    batch.remove(global, &key_bytes);
    Ok(())
}

/// Get stats that haven't been rolled up yet
fn get_unrolled_collection_seen(global: &PartitionHandle, collection: Nsid) -> anyhow::Result<u64> {
    let range =
        if let Some(cursor_value) = get_static::<RollupCursorKey, RollupCursorValue>(global)? {
            eprintln!("found existing cursor");
            let key: ByCursorSeenKey = cursor_value.into();
            key.range_from()?
        } else {
            eprintln!("cursor from start.");
            ByCursorSeenKey::full_range()?
        };

    let mut collection_total = 0;

    let mut scanned = 0;
    let mut rolled = 0;

    for pair in global.range(range) {
        let (key_bytes, value_bytes) = pair?;
        let key = db_complete::<ByCursorSeenKey>(&key_bytes)?;
        let val = db_complete::<ByCursorSeenValue>(&value_bytes)?;

        if *key.collection() == collection {
            let SeenCounter(n) = val;
            collection_total += n;
            rolled += 1;
        }
        scanned += 1;
    }

    eprintln!("scanned: {scanned}, rolled: {rolled}");

    Ok(collection_total)
}

fn get_unrolled_top_collections(global: &PartitionHandle) -> anyhow::Result<HashMap<String, u64>> {
    let range =
        if let Some(cursor_value) = get_static::<RollupCursorKey, RollupCursorValue>(global)? {
            eprintln!("found existing cursor");
            let key: ByCursorSeenKey = cursor_value.into();
            key.range_from()?
        } else {
            eprintln!("cursor from start.");
            ByCursorSeenKey::full_range()?
        };

    let mut res = HashMap::new();
    let mut scanned = 0;

    for pair in global.range(range) {
        let (key_bytes, value_bytes) = pair?;
        let key = db_complete::<ByCursorSeenKey>(&key_bytes)?;
        let SeenCounter(n) = db_complete(&value_bytes)?;

        *res.entry(key.collection().to_string()).or_default() += n;

        scanned += 1;
    }

    eprintln!("scanned: {scanned} seen-counts.");

    Ok(res)
}

impl DBWriter {
    fn write_batch(self, _event_batch: EventBatch, _last: Option<Cursor>) -> anyhow::Result<()> {
        todo!();
        // let mut db_batch = self.keyspace.batch();
        // self.add_record_creates(&mut db_batch, event_batch.record_creates)?;
        // self.add_record_modifies(&mut db_batch, event_batch.record_modifies)?;
        // self.add_account_removes(&mut db_batch, event_batch.account_removes)?;
        // if let Some(cursor) = last {
        //     insert_batch_static::<JetstreamCursorKey>(&mut db_batch, &self.global, cursor)?;
        // }
        // log::info!("write: committing write batch...");
        // let r = db_batch.commit();
        // log::info!("write: commit result: {r:?}");
        // r?;
        // Ok(())
    }

    fn write_rw(
        self,
        db_batch: &mut FjallBatch,
        mod_key: ModQueueItemKey,
        mod_value: ModQueueItemValue,
    ) -> anyhow::Result<usize> {
        // update the current rw cursor to this item (atomically with the batch if it succeeds)
        let mod_cursor: Cursor = (&mod_key).into();
        insert_batch_static::<ModCursorKey>(db_batch, &self.global, mod_cursor.clone())?;

        let items_modified = match mod_value {
            ModQueueItemValue::DeleteAccount(did) => {
                log::trace!("rw: batcher: delete account...");
                let (items, finished) = self.delete_account(db_batch, mod_cursor, did)?;
                log::trace!("rw: batcher: back from delete account (finished? {finished})");
                if finished {
                    // only remove the queued rw task if we have actually completed its account removal work
                    remove_batch::<ModQueueItemKey>(db_batch, &self.global, mod_key)?;
                    items + 1
                } else {
                    items
                }
            }
            ModQueueItemValue::DeleteRecord(did, collection, rkey) => {
                log::trace!("rw: batcher: delete record...");
                let items = self.delete_record(db_batch, mod_cursor, did, collection, rkey)?;
                log::trace!("rw: batcher: back from delete record");
                remove_batch::<ModQueueItemKey>(db_batch, &self.global, mod_key)?;
                items + 1
            }
            ModQueueItemValue::UpdateRecord(did, collection, rkey, record) => {
                let items =
                    self.update_record(db_batch, mod_cursor, did, collection, rkey, record)?;
                remove_batch::<ModQueueItemKey>(db_batch, &self.global, mod_key)?;
                items + 1
            }
        };
        Ok(items_modified)
    }

    fn update_record(
        &self,
        db_batch: &mut FjallBatch,
        cursor: Cursor,
        did: Did,
        collection: Nsid,
        rkey: RecordKey,
        record: serde_json::Value,
    ) -> anyhow::Result<usize> {
        // 1. delete any existing versions older than us
        let items_deleted = self.delete_record(
            db_batch,
            cursor.clone(),
            did.clone(),
            collection.clone(),
            rkey.clone(),
        )?;

        // 2. insert the updated version, at our new cursor
        self.add_record(db_batch, cursor, did, collection, rkey, record)?;

        let items_total = items_deleted + 1;
        Ok(items_total)
    }

    fn delete_record(
        &self,
        db_batch: &mut FjallBatch,
        cursor: Cursor,
        did: Did,
        collection: Nsid,
        rkey: RecordKey,
    ) -> anyhow::Result<usize> {
        let key_prefix_bytes =
            ByIdKey::record_prefix(did.clone(), collection.clone(), rkey.clone()).to_db_bytes()?;

        // put the cursor of the actual deletion event in to prevent prefix iter from touching newer docs
        let key_limit =
            ByIdKey::new(did, collection.clone(), rkey, cursor.clone()).to_db_bytes()?;

        let mut items_removed = 0;

        log::trace!("delete_record: iterate over up to current cursor...");

        for (i, pair) in self.global.range(key_prefix_bytes..key_limit).enumerate() {
            log::trace!("delete_record iter {i}: found");
            // find all (hopefully 1)
            let (key_bytes, _) = pair?;
            let key = db_complete::<ByIdKey>(&key_bytes)?;
            let found_cursor = key.cursor();
            if found_cursor > cursor {
                // we are *only* allowed to delete records that came before the record delete event
                // log::trace!("delete_record: found (and ignoring) newer version(s). key: {key:?}");
                panic!("wtf, found newer version than cursor limit we tried to set.");
                // break;
            }

            // remove the by_id entry
            db_batch.remove(&self.global, key_bytes);

            // remove its record sample
            let by_collection_key_bytes =
                ByCollectionKey::new(collection.clone(), found_cursor).to_db_bytes()?;
            db_batch.remove(&self.global, by_collection_key_bytes);

            items_removed += 1;
        }

        // if items_removed > 1 {
        //     log::trace!("odd, removed {items_removed} records for one record removal:");
        //     for (i, pair) in self.global.prefix(&key_prefix_bytes).enumerate() {
        //         // find all (hopefully 1)
        //         let (key_bytes, _) = pair?;
        //         let found_cursor = db_complete::<ByIdKey>(&key_bytes)?.cursor();
        //         if found_cursor > cursor {
        //             break;
        //         }

        //         let key = db_complete::<ByIdKey>(&key_bytes)?;
        //         log::trace!("  {i}: key {key:?}");
        //     }
        // }
        Ok(items_removed)
    }

    fn delete_account(
        &self,
        db_batch: &mut FjallBatch,
        cursor: Cursor,
        did: Did,
    ) -> anyhow::Result<(usize, bool)> {
        let key_prefix_bytes = ByIdKey::did_prefix(did).to_db_bytes()?;

        let mut items_added = 0;

        for pair in self.global.prefix(&key_prefix_bytes) {
            let (key_bytes, _) = pair?;

            let (_, collection, _rkey, found_cursor) = db_complete::<ByIdKey>(&key_bytes)?.into();
            if found_cursor > cursor {
                log::trace!(
                    "delete account: found (and ignoring) newer records than the delete event??"
                );
                continue;
            }

            // remove the by_id entry
            db_batch.remove(&self.global, key_bytes);

            // remove its record sample
            let by_collection_key_bytes =
                ByCollectionKey::new(collection, found_cursor).to_db_bytes()?;
            db_batch.remove(&self.global, by_collection_key_bytes);

            items_added += 1;
            if items_added >= MAX_BATCHED_RW_ITEMS {
                return Ok((items_added, false)); // there might be more records but we've done enough for this batch
            }
        }

        Ok((items_added, true))
    }

    fn add_record_creates(
        &self,
        _db_batch: &mut FjallBatch,
        _record_creates: HashMap<Nsid, ()>,
    ) -> anyhow::Result<()> {
        todo!();
        // for (
        //     collection,
        //     CollectionSamples {
        //         total_seen,
        //         samples,
        //     },
        // ) in record_creates.into_iter()
        // {
        //     if let Some(last_record) = &samples.back() {
        //         db_batch.insert(
        //             &self.global,
        //             ByCursorSeenKey::new(last_record.cursor.clone(), collection.clone())
        //                 .to_db_bytes()?,
        //             ByCursorSeenValue::new(total_seen as u64).to_db_bytes()?,
        //         );
        //     } else {
        //         log::error!(
        //             "collection samples should only exist when at least one sample has been added"
        //         );
        //     }

        //     for CreateRecord {
        //         did,
        //         rkey,
        //         cursor,
        //         record,
        //     } in samples.into_iter().rev()
        //     {
        //         self.add_record(db_batch, cursor, did, collection.clone(), rkey, record)?;
        //     }
        // }
        // Ok(())
    }

    fn add_record(
        &self,
        db_batch: &mut FjallBatch,
        cursor: Cursor,
        did: Did,
        collection: Nsid,
        rkey: RecordKey,
        record: serde_json::Value,
    ) -> anyhow::Result<()> {
        // ["by_collection"|collection|js_cursor] => [did|rkey|record]
        db_batch.insert(
            &self.global,
            ByCollectionKey::new(collection.clone(), cursor.clone()).to_db_bytes()?,
            ByCollectionValue::new(did.clone(), rkey.clone(), record).to_db_bytes()?,
        );

        // ["by_id"|did|collection|rkey|js_cursor] => [] // required to support deletes; did first prefix for account deletes.
        db_batch.insert(
            &self.global,
            ByIdKey::new(did, collection.clone(), rkey, cursor).to_db_bytes()?,
            ByIdValue::default().to_db_bytes()?,
        );

        Ok(())
    }

    fn add_record_modifies(
        &self,
        _db_batch: &mut FjallBatch,
        _record_modifies: Vec<()>,
    ) -> anyhow::Result<()> {
        todo!();
        // for modification in record_modifies {
        //     let (cursor, db_val) = match modification {
        //         ModifyRecord::Update(u) => (
        //             u.cursor,
        //             ModQueueItemValue::UpdateRecord(u.did, u.collection, u.rkey, u.record),
        //         ),
        //         ModifyRecord::Delete(d) => (
        //             d.cursor,
        //             ModQueueItemValue::DeleteRecord(d.did, d.collection, d.rkey),
        //         ),
        //     };
        //     db_batch.insert(
        //         &self.global,
        //         ModQueueItemKey::new(cursor).to_db_bytes()?,
        //         db_val.to_db_bytes()?,
        //     );
        // }
        // Ok(())
    }

    fn add_account_removes(
        &self,
        db_batch: &mut FjallBatch,
        account_removes: Vec<DeleteAccount>,
    ) -> anyhow::Result<()> {
        for deletion in account_removes {
            db_batch.insert(
                &self.global,
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
    pub global_approximate_len: usize,
}

struct DBWriter {
    keyspace: Keyspace,
    global: PartitionHandle,
}

////////// temp stuff to remove:

fn summarize_batch(batch: &EventBatch) -> String {
    format!(
        "batch of {: >3} samples from {: >4} records in {: >2} collections from ~{: >4} DIDs, {} acct removes, cursor {: <12?}",
        batch.total_records(),
        batch.total_seen(),
        batch.total_collections(),
        batch.estimate_dids(),
        batch.account_removes(),
        batch.latest_cursor().map(|c| c.elapsed()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CollectionCommits, UFOsCommit};
    use jetstream::events::{CommitEvent, CommitOp};
    use jetstream::exports::Cid;
    use serde_json::value::RawValue;

    fn fjall_db() -> (FjallReader, FjallWriter) {
        let (read, write, _) = FjallStorage::init(
            tempfile::tempdir().unwrap(),
            "offline test (no real jetstream endpoint)".to_string(),
            false,
            FjallConfig { temp: true },
        )
        .unwrap();
        (read, write)
    }

    #[derive(Debug, Default)]
    struct TestBatch {
        pub batch: EventBatch,
    }

    impl TestBatch {
        pub fn create(
            &mut self,
            did: &str,
            collection: &str,
            rkey: &str,
            record: &str,
            rev: Option<&str>,
            cid: Option<Cid>,
            cursor: u64,
        ) -> Nsid {
            let did = Did::new(did.to_string()).unwrap();
            let collection = Nsid::new(collection.to_string()).unwrap();
            let record = RawValue::from_string(record.to_string()).unwrap();
            let cid = cid.unwrap_or(
                "bafyreidofvwoqvd2cnzbun6dkzgfucxh57tirf3ohhde7lsvh4fu3jehgy"
                    .parse()
                    .unwrap(),
            );

            let event = CommitEvent {
                collection,
                rkey: RecordKey::new(rkey.to_string()).unwrap(),
                rev: rev.unwrap_or("asdf").to_string(),
                operation: CommitOp::Create,
                record: Some(record),
                cid: Some(cid),
            };

            let (commit, collection) =
                UFOsCommit::from_commit_info(event, did.clone(), Cursor::from_raw_u64(cursor))
                    .unwrap();

            self.batch
                .commits_by_nsid
                .entry(collection.clone())
                .or_default()
                .truncating_insert(commit, 1);

            collection
        }
    }

    #[test]
    fn test_hello() -> anyhow::Result<()> {
        let (read, mut write) = fjall_db();
        write.insert_batch(EventBatch::default())?;
        let total = read.get_total_by_collection(&Nsid::new("a.b.c".to_string()).unwrap())?;
        assert_eq!(total, 0);
        Ok(())
    }

    #[test]
    fn test_insert_one() -> anyhow::Result<()> {
        let (read, mut write) = fjall_db();

        let mut batch = TestBatch::default();
        let collection = batch.create(
            "did:plc:inze6wrmsm7pjl7yta3oig77",
            "a.b.c",
            "asdf",
            "{}",
            Some("rev-z"),
            None,
            100,
        );
        write.insert_batch(batch.batch)?;

        let total = read.get_total_by_collection(&collection)?;
        assert_eq!(total, 1);
        let total = read.get_total_by_collection(&Nsid::new("d.e.f".to_string()).unwrap())?;
        assert_eq!(total, 0);

        let total = read.get_dids_by_collection(&collection)?;
        assert_eq!(total, 1);
        let total = read.get_dids_by_collection(&Nsid::new("d.e.f".to_string()).unwrap())?;
        assert_eq!(total, 0);

        // let records = read.get_records_by_collections(&vec![collection], 2);
        // assert_eq!(records.len, 1);

        // let records = read.get_records_by_collections(&vec![&Nsid::new("d.e.f".to_string()).unwrap()], 2);
        // assert_eq!(records.len, 0);

        Ok(())
    }
}

use crate::consumer::LimitedBatch;
use crate::db_types::{db_complete, DbBytes, DbStaticStr, EncodingError, StaticStr};
use crate::error::StorageError;
use crate::storage::{StorageResult, StorageWhatever, StoreReader, StoreWriter};
use crate::store_types::{
    AllTimeRollupKey, ByCollectionKey, ByCollectionValue, ByCursorSeenKey, ByCursorSeenValue,
    ByIdKey, ByIdValue, CountsValue, DeleteAccountQueueKey, DeleteAccountQueueVal,
    HourTruncatedCursor, HourlyRollupKey, JetstreamCursorKey, JetstreamCursorValue,
    JetstreamEndpointKey, JetstreamEndpointValue, LiveCountsKey, ModCursorKey, ModCursorValue,
    ModQueueItemKey, ModQueueItemStringValue, ModQueueItemValue, NewRollupCursorKey,
    NewRollupCursorValue, NsidRecordFeedKey, NsidRecordFeedVal, RecordLocationKey,
    RecordLocationMeta, RecordLocationVal, RecordRawValue, RollupCursorKey, RollupCursorValue,
    SeenCounter, TakeoffKey, WeekTruncatedCursor, WeeklyRollupKey,
};
use crate::{CommitAction, Did, EventBatch, Nsid, RecordKey, UFOsRecord};
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

const MAX_BATCHED_CLEANUP_SIZE: usize = 1024; // try to commit progress for longer feeds
const MAX_BATCHED_ACCOUNT_DELETE_RECORDS: usize = 1024;
const MAX_BATCHED_ROLLUP_COUNTS: usize = 256;

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
 *
 * Partition: 'rollups'
 *
 * - Live (batched) records counts and dids estimate per collection
 *      key: "live_counts" || u64 || nullstr (js_cursor, nsid)
 *      val: u64 || HLL (count (not cursor), estimator)
 *
 * - Hourly total record counts and dids estimate per collection
 *      key: "hourly_counts" || u64 || nullstr (hour, nsid)
 *      val: u64 || HLL (count (not cursor), estimator)
 *
 * - Weekly total record counts and dids estimate per collection
 *      key: "weekly_counts" || u64 || nullstr (hour, nsid)
 *      val: u64 || HLL (count (not cursor), estimator)
 *
 * - All-time total record counts and dids estimate per collection
 *      key: "ever_counts" || nullstr (nsid)
 *      val: u64 || HLL (count (not cursor), estimator)
 *
 * - TODO: sorted indexes for all-times?
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
    ) -> StorageResult<(FjallReader, FjallWriter, Option<Cursor>)> {
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
            keyspace: keyspace.clone(),
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
    keyspace: Keyspace,
    global: PartitionHandle,
    feeds: PartitionHandle,
    records: PartitionHandle,
    rollups: PartitionHandle,
}

impl StoreReader for FjallReader {
    fn get_counts_by_collection(&self, collection: &Nsid) -> StorageResult<(u64, u64)> {
        // 0. grab a snapshot in case rollups happen while we're working
        let instant = self.keyspace.instant();
        let global = self.global.snapshot_at(instant);
        let rollups = self.rollups.snapshot_at(instant);

        // 1. all-time counts
        let all_time_key = AllTimeRollupKey::new(collection).to_db_bytes()?;
        let mut total_counts = rollups
            .get(&all_time_key)?
            .as_deref()
            .map(db_complete::<CountsValue>)
            .transpose()?
            .unwrap_or_default();

        // 2. live counts that haven't been rolled into all-time yet.
        let rollup_cursor =
            get_snapshot_static_neu::<NewRollupCursorKey, NewRollupCursorValue>(&global)?.ok_or(
                StorageError::BadStateError("Could not find current rollup cursor".to_string()),
            )?;

        let full_range = LiveCountsKey::range_from_cursor(rollup_cursor)?;
        for kv in rollups.range(full_range) {
            let (key_bytes, val_bytes) = kv?;
            let key = db_complete::<LiveCountsKey>(&key_bytes)?;
            if key.collection() == collection {
                let counts = db_complete::<CountsValue>(&val_bytes)?;
                total_counts.merge(&counts);
            }
        }
        Ok((
            total_counts.records(),
            total_counts.dids().estimate() as u64,
        ))
    }

    fn get_records_by_collections(
        &self,
        collections: &[&Nsid],
        limit: usize,
    ) -> StorageResult<Vec<UFOsRecord>> {
        if collections.is_empty() {
            return Ok(vec![]);
        } else if collections.len() > 1 {
            todo!()
        }

        let collection = collections[0];

        let prefix = NsidRecordFeedKey::from_prefix_to_db_bytes(collection)?;
        let collected = 0;
        let mut out = vec![];
        for kv in self.feeds.prefix(prefix).rev() {
            let (key_bytes, val_bytes) = kv?;
            let feed_key = db_complete::<NsidRecordFeedKey>(&key_bytes)?;
            let feed_val = db_complete::<NsidRecordFeedVal>(&val_bytes)?;
            let location_key: RecordLocationKey = (&feed_key, &feed_val).into();

            let Some(location_val_bytes) = self.records.get(location_key.to_db_bytes()?)? else {
                // record was deleted (hopefully)
                continue;
            };

            let (meta, n) = RecordLocationMeta::from_db_bytes(&location_val_bytes)?;

            if meta.cursor() != feed_key.cursor() {
                // older/different version
                continue;
            }
            if meta.rev != feed_val.rev() {
                // weird...
                log::warn!("record lookup: cursor match but rev did not...? excluding.");
                continue;
            }
            let Some(raw_value_bytes) = location_val_bytes.get(n..) else {
                log::warn!(
                    "record lookup: found record but could not get bytes to decode the record??"
                );
                continue;
            };
            let rawval = db_complete::<RecordRawValue>(raw_value_bytes)?;
            out.push(UFOsRecord {
                collection: feed_key.collection().clone(),
                cursor: feed_key.cursor(),
                did: feed_val.did().clone(),
                rkey: feed_val.rkey().clone(),
                rev: meta.rev.to_string(),
                record: rawval.try_into()?,
                is_update: meta.is_update,
            });

            if collected >= limit {
                break;
            }
        }

        Ok(out)
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
    pub fn step_rollup(&mut self) -> StorageResult<usize> {
        let rollup_cursor =
            get_static_neu::<NewRollupCursorKey, NewRollupCursorValue>(&self.global)?.ok_or(
                StorageError::BadStateError("Could not find current rollup cursor".to_string()),
            )?;

        // timelies
        let live_counts_range = LiveCountsKey::range_from_cursor(rollup_cursor)?;
        let mut timely_iter = self.rollups.range(live_counts_range).peekable();

        let timely_next_cursor = timely_iter
            .peek_mut()
            .map(|kv| -> StorageResult<Cursor> {
                match kv {
                    Err(e) => Err(std::mem::replace(e, fjall::Error::Poisoned))?,
                    Ok((key_bytes, _)) => {
                        let key = db_complete::<LiveCountsKey>(key_bytes)?;
                        Ok(key.cursor())
                    }
                }
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
                db_complete::<DeleteAccountQueueKey>(&key_bytes)
                    .map(|k| (k.suffix, key_bytes, val_bytes))
            })
            .transpose()?;

        let cursors_stepped = match (timely_next_cursor, next_delete) {
            (
                Some(timely_next_cursor),
                Some((delete_cursor, delete_key_bytes, delete_val_bytes)),
            ) => {
                if timely_next_cursor < delete_cursor {
                    self.rollup_live_counts(
                        timely_iter,
                        Some(delete_cursor),
                        MAX_BATCHED_ROLLUP_COUNTS,
                    )?
                } else {
                    self.rollup_delete_account(delete_cursor, &delete_key_bytes, &delete_val_bytes)?
                }
            }
            (Some(_), None) => {
                self.rollup_live_counts(timely_iter, None, MAX_BATCHED_ROLLUP_COUNTS)?
            }
            (None, Some((delete_cursor, delete_key_bytes, delete_val_bytes))) => {
                self.rollup_delete_account(delete_cursor, &delete_key_bytes, &delete_val_bytes)?
            }
            (None, None) => 0,
        };

        Ok(cursors_stepped)
    }

    fn rollup_delete_account(
        &mut self,
        cursor: Cursor,
        key_bytes: &[u8],
        val_bytes: &[u8],
    ) -> StorageResult<usize> {
        let did = db_complete::<DeleteAccountQueueVal>(val_bytes)?;
        self.delete_account(&did)?;
        let mut batch = self.keyspace.batch();
        batch.remove(&self.queues, key_bytes);
        insert_batch_static_neu::<NewRollupCursorKey>(&mut batch, &self.global, cursor)?;
        batch.commit()?;
        Ok(1)
    }

    fn rollup_live_counts(
        &mut self,
        timelies: impl Iterator<Item = Result<(fjall::Slice, fjall::Slice), fjall::Error>>,
        cursor_exclusive_limit: Option<Cursor>,
        rollup_limit: usize,
    ) -> StorageResult<usize> {
        // current strategy is to buffer counts in mem before writing the rollups
        // we *could* read+write every single batch to rollup.. but their merge is associative so
        // ...so save the db some work up front? is this worth it? who knows...

        #[derive(Eq, Hash, PartialEq)]
        enum Rollup {
            Hourly(HourTruncatedCursor),
            Weekly(WeekTruncatedCursor),
            AllTime,
        }

        let mut batch = self.keyspace.batch();
        let mut cursors_advanced = 0;
        let mut last_cursor = Cursor::from_start();
        let mut counts_by_rollup: HashMap<(Nsid, Rollup), CountsValue> = HashMap::new();

        for (i, kv) in timelies.enumerate() {
            if i >= rollup_limit {
                break;
            }

            let (key_bytes, val_bytes) = kv?;
            let key = db_complete::<LiveCountsKey>(&key_bytes)?;

            if cursor_exclusive_limit
                .map(|limit| key.cursor() > limit)
                .unwrap_or(false)
            {
                break;
            }

            batch.remove(&self.rollups, key_bytes);
            let val = db_complete::<CountsValue>(&val_bytes)?;
            counts_by_rollup
                .entry((
                    key.collection().clone(),
                    Rollup::Hourly(key.cursor().into()),
                ))
                .or_default()
                .merge(&val);
            counts_by_rollup
                .entry((
                    key.collection().clone(),
                    Rollup::Weekly(key.cursor().into()),
                ))
                .or_default()
                .merge(&val);
            counts_by_rollup
                .entry((key.collection().clone(), Rollup::AllTime))
                .or_default()
                .merge(&val);

            cursors_advanced += 1;
            last_cursor = key.cursor();
        }

        for ((nsid, rollup), counts) in counts_by_rollup {
            let key_bytes = match rollup {
                Rollup::Hourly(hourly_cursor) => {
                    HourlyRollupKey::new(hourly_cursor, &nsid).to_db_bytes()?
                }
                Rollup::Weekly(weekly_cursor) => {
                    WeeklyRollupKey::new(weekly_cursor, &nsid).to_db_bytes()?
                }
                Rollup::AllTime => AllTimeRollupKey::new(&nsid).to_db_bytes()?,
            };
            let mut rolled = self
                .rollups
                .get(&key_bytes)?
                .as_deref()
                .map(db_complete::<CountsValue>)
                .transpose()?
                .unwrap_or_default();
            rolled.merge(&counts);
            batch.insert(&self.rollups, &key_bytes, &rolled.to_db_bytes()?);
        }

        insert_batch_static_neu::<NewRollupCursorKey>(&mut batch, &self.global, last_cursor)?;

        batch.commit()?;
        Ok(cursors_advanced)
    }
}

impl StoreWriter for FjallWriter {
    fn insert_batch<const LIMIT: usize>(
        &mut self,
        event_batch: EventBatch<LIMIT>,
    ) -> StorageResult<()> {
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
            let live_counts_key: LiveCountsKey = (latest, &nsid).into();
            let counts_value = CountsValue::new(commits.total_seen as u64, commits.dids_estimate);
            batch.insert(
                &self.rollups,
                &live_counts_key.to_db_bytes()?,
                &counts_value.to_db_bytes()?,
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

    fn trim_collection(
        &mut self,
        collection: &Nsid,
        limit: usize,
        // TODO: could add a start cursor limit to avoid iterating deleted stuff at the start (/end)
    ) -> StorageResult<()> {
        let mut dangling_feed_keys_cleaned = 0;
        let mut records_deleted = 0;

        let mut batch = self.keyspace.batch();

        let prefix = NsidRecordFeedKey::from_prefix_to_db_bytes(collection)?;
        let mut found = 0;
        for kv in self.feeds.prefix(prefix).rev() {
            let (key_bytes, val_bytes) = kv?;
            let feed_key = db_complete::<NsidRecordFeedKey>(&key_bytes)?;
            let feed_val = db_complete::<NsidRecordFeedVal>(&val_bytes)?;
            let location_key: RecordLocationKey = (&feed_key, &feed_val).into();
            let location_key_bytes = location_key.to_db_bytes()?;

            let Some(location_val_bytes) = self.records.get(&location_key_bytes)? else {
                // record was deleted (hopefully)
                batch.remove(&self.feeds, &location_key_bytes);
                dangling_feed_keys_cleaned += 1;
                continue;
            };

            let (meta, _) = RecordLocationMeta::from_db_bytes(&location_val_bytes)?;

            if meta.cursor() != feed_key.cursor() {
                // older/different version
                batch.remove(&self.feeds, &location_key_bytes);
                dangling_feed_keys_cleaned += 1;
                continue;
            }
            if meta.rev != feed_val.rev() {
                // weird...
                log::warn!("record lookup: cursor match but rev did not...? removing.");
                batch.remove(&self.feeds, &location_key_bytes);
                dangling_feed_keys_cleaned += 1;
                continue;
            }

            if batch.len() >= MAX_BATCHED_CLEANUP_SIZE {
                batch.commit()?;
                batch = self.keyspace.batch();
            }

            found += 1;
            if found <= limit {
                continue;
            }

            batch.remove(&self.feeds, &location_key_bytes);
            batch.remove(&self.records, &location_key_bytes);
            records_deleted += 1;
        }

        batch.commit()?;

        log::info!("trim_collection ({collection:?}) removed {dangling_feed_keys_cleaned} dangling feed entries and {records_deleted} records");
        Ok(())
    }

    fn delete_account(&mut self, did: &Did) -> Result<usize, StorageError> {
        let mut records_deleted = 0;
        let mut batch = self.keyspace.batch();
        let prefix = RecordLocationKey::from_prefix_to_db_bytes(did)?;
        for kv in self.records.prefix(prefix) {
            let (key_bytes, _) = kv?;
            batch.remove(&self.records, key_bytes);
            records_deleted += 1;
            if batch.len() >= MAX_BATCHED_ACCOUNT_DELETE_RECORDS {
                batch.commit()?;
                batch = self.keyspace.batch();
            }
        }
        batch.commit()?;
        Ok(records_deleted)
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
    pub async fn receive(&self, mut receiver: Receiver<LimitedBatch>) -> anyhow::Result<()> {
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
                // let last = event_batch.last_jetstream_cursor; // TODO: get this from the data. track last in consumer. compute or track first.

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
        let range = ModQueueItemKey::new(mod_cursor).range_to_prefix_end()?;

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
fn get_static_neu<K: StaticStr, V: DbBytes>(global: &PartitionHandle) -> StorageResult<Option<V>> {
    let key_bytes = DbStaticStr::<K>::default().to_db_bytes()?;
    let value = global
        .get(&key_bytes)?
        .map(|value_bytes| db_complete(&value_bytes))
        .transpose()?;
    Ok(value)
}

/// Get a value from a fixed key
fn get_snapshot_static_neu<K: StaticStr, V: DbBytes>(
    global: &fjall::Snapshot,
) -> StorageResult<Option<V>> {
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
) -> StorageResult<()> {
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

/// Set a value to a fixed key
fn insert_batch_static_neu<K: StaticStr>(
    batch: &mut FjallBatch,
    global: &PartitionHandle,
    value: impl DbBytes,
) -> StorageResult<()> {
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
    fn write_rw(
        self,
        db_batch: &mut FjallBatch,
        mod_key: ModQueueItemKey,
        mod_value: ModQueueItemValue,
    ) -> anyhow::Result<usize> {
        // update the current rw cursor to this item (atomically with the batch if it succeeds)
        let mod_cursor: Cursor = (&mod_key).into();
        insert_batch_static::<ModCursorKey>(db_batch, &self.global, mod_cursor)?;

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
            cursor,
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
        let key_limit = ByIdKey::new(did, collection.clone(), rkey, cursor).to_db_bytes()?;

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
            ByCollectionKey::new(collection.clone(), cursor).to_db_bytes()?,
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
}

#[derive(Debug, serde::Serialize, schemars::JsonSchema)]
pub struct StorageInfo {
    pub keyspace_disk_space: u64,
    pub keyspace_journal_count: usize,
    pub keyspace_sequence: u64,
    pub global_approximate_len: usize,
}

struct DBWriter {
    global: PartitionHandle,
}

////////// temp stuff to remove:

fn summarize_batch<const LIMIT: usize>(batch: &EventBatch<LIMIT>) -> String {
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
    use crate::{DeleteAccount, UFOsCommit};
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

    const TEST_BATCH_LIMIT: usize = 16;

    #[derive(Debug, Default)]
    struct TestBatch {
        pub batch: EventBatch<TEST_BATCH_LIMIT>,
    }

    impl TestBatch {
        #[allow(clippy::too_many_arguments)]
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
                .truncating_insert(commit)
                .unwrap();

            collection
        }
        #[allow(clippy::too_many_arguments)]
        pub fn update(
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
                operation: CommitOp::Update,
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
                .truncating_insert(commit)
                .unwrap();

            collection
        }
        #[allow(clippy::too_many_arguments)]
        pub fn delete(
            &mut self,
            did: &str,
            collection: &str,
            rkey: &str,
            rev: Option<&str>,
            cursor: u64,
        ) -> Nsid {
            let did = Did::new(did.to_string()).unwrap();
            let collection = Nsid::new(collection.to_string()).unwrap();
            let event = CommitEvent {
                collection,
                rkey: RecordKey::new(rkey.to_string()).unwrap(),
                rev: rev.unwrap_or("asdf").to_string(),
                operation: CommitOp::Delete,
                record: None,
                cid: None,
            };

            let (commit, collection) =
                UFOsCommit::from_commit_info(event, did, Cursor::from_raw_u64(cursor)).unwrap();

            self.batch
                .commits_by_nsid
                .entry(collection.clone())
                .or_default()
                .truncating_insert(commit)
                .unwrap();

            collection
        }
        pub fn delete_account(&mut self, did: &str, cursor: u64) -> Did {
            let did = Did::new(did.to_string()).unwrap();
            self.batch.account_removes.push(DeleteAccount {
                did: did.clone(),
                cursor: Cursor::from_raw_u64(cursor),
            });
            did
        }
    }

    #[test]
    fn test_hello() -> anyhow::Result<()> {
        let (read, mut write) = fjall_db();
        write.insert_batch::<TEST_BATCH_LIMIT>(EventBatch::default())?;
        let (records, dids) =
            read.get_counts_by_collection(&Nsid::new("a.b.c".to_string()).unwrap())?;
        assert_eq!(records, 0);
        assert_eq!(dids, 0);
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

        let (records, dids) = read.get_counts_by_collection(&collection)?;
        assert_eq!(records, 1);
        assert_eq!(dids, 1);
        let (records, dids) =
            read.get_counts_by_collection(&Nsid::new("d.e.f".to_string()).unwrap())?;
        assert_eq!(records, 0);
        assert_eq!(dids, 0);

        let records = read.get_records_by_collections(&[&collection], 2)?;
        assert_eq!(records.len(), 1);
        let rec = &records[0];
        assert_eq!(rec.record.get(), "{}");
        assert!(!rec.is_update);

        let records =
            read.get_records_by_collections(&[&Nsid::new("d.e.f".to_string()).unwrap()], 2)?;
        assert_eq!(records.len(), 0);

        Ok(())
    }

    #[test]
    fn test_update_one() -> anyhow::Result<()> {
        let (read, mut write) = fjall_db();

        let mut batch = TestBatch::default();
        let collection = batch.create(
            "did:plc:inze6wrmsm7pjl7yta3oig77",
            "a.b.c",
            "rkey-asdf",
            "{}",
            Some("rev-a"),
            None,
            100,
        );
        write.insert_batch(batch.batch)?;

        let mut batch = TestBatch::default();
        batch.update(
            "did:plc:inze6wrmsm7pjl7yta3oig77",
            "a.b.c",
            "rkey-asdf",
            r#"{"ch":  "ch-ch-ch-changes"}"#,
            Some("rev-z"),
            None,
            101,
        );
        write.insert_batch(batch.batch)?;

        let (records, dids) = read.get_counts_by_collection(&collection)?;
        assert_eq!(records, 1);
        assert_eq!(dids, 1);

        let records = read.get_records_by_collections(&[&collection], 2)?;
        assert_eq!(records.len(), 1);
        let rec = &records[0];
        assert_eq!(rec.record.get(), r#"{"ch":  "ch-ch-ch-changes"}"#);
        assert!(rec.is_update);
        Ok(())
    }

    #[test]
    fn test_delete_one() -> anyhow::Result<()> {
        let (read, mut write) = fjall_db();

        let mut batch = TestBatch::default();
        let collection = batch.create(
            "did:plc:inze6wrmsm7pjl7yta3oig77",
            "a.b.c",
            "rkey-asdf",
            "{}",
            Some("rev-a"),
            None,
            100,
        );
        write.insert_batch(batch.batch)?;

        let mut batch = TestBatch::default();
        batch.delete(
            "did:plc:inze6wrmsm7pjl7yta3oig77",
            "a.b.c",
            "rkey-asdf",
            Some("rev-z"),
            101,
        );
        write.insert_batch(batch.batch)?;

        let (records, dids) = read.get_counts_by_collection(&collection)?;
        assert_eq!(records, 1);
        assert_eq!(dids, 1);

        let records = read.get_records_by_collections(&[&collection], 2)?;
        assert_eq!(records.len(), 0);

        Ok(())
    }

    #[test]
    fn test_collection_trim() -> anyhow::Result<()> {
        let (read, mut write) = fjall_db();

        let mut batch = TestBatch::default();
        batch.create(
            "did:plc:inze6wrmsm7pjl7yta3oig77",
            "a.a.a",
            "rkey-aaa",
            "{}",
            Some("rev-aaa"),
            None,
            10_000,
        );
        let mut last_b_cursor;
        for i in 1..=10 {
            last_b_cursor = 11_000 + i;
            batch.create(
                &format!("did:plc:inze6wrmsm7pjl7yta3oig7{}", i % 3),
                "a.a.b",
                &format!("rkey-bbb-{i}"),
                &format!(r#"{{"n": {i}}}"#),
                Some(&format!("rev-bbb-{i}")),
                None,
                last_b_cursor,
            );
        }
        batch.create(
            "did:plc:inze6wrmsm7pjl7yta3oig77",
            "a.a.c",
            "rkey-ccc",
            "{}",
            Some("rev-ccc"),
            None,
            12_000,
        );

        write.insert_batch(batch.batch)?;

        let records =
            read.get_records_by_collections(&[&Nsid::new("a.a.a".to_string()).unwrap()], 100)?;
        assert_eq!(records.len(), 1);
        let records =
            read.get_records_by_collections(&[&Nsid::new("a.a.b".to_string()).unwrap()], 100)?;
        assert_eq!(records.len(), 10);
        let records =
            read.get_records_by_collections(&[&Nsid::new("a.a.c".to_string()).unwrap()], 100)?;
        assert_eq!(records.len(), 1);
        let records =
            read.get_records_by_collections(&[&Nsid::new("a.a.d".to_string()).unwrap()], 100)?;
        assert_eq!(records.len(), 0);

        write.trim_collection(&Nsid::new("a.a.a".to_string()).unwrap(), 6)?;
        write.trim_collection(&Nsid::new("a.a.b".to_string()).unwrap(), 6)?;
        write.trim_collection(&Nsid::new("a.a.c".to_string()).unwrap(), 6)?;
        write.trim_collection(&Nsid::new("a.a.d".to_string()).unwrap(), 6)?;

        let records =
            read.get_records_by_collections(&[&Nsid::new("a.a.a".to_string()).unwrap()], 100)?;
        assert_eq!(records.len(), 1);
        let records =
            read.get_records_by_collections(&[&Nsid::new("a.a.b".to_string()).unwrap()], 100)?;
        assert_eq!(records.len(), 6);
        let records =
            read.get_records_by_collections(&[&Nsid::new("a.a.c".to_string()).unwrap()], 100)?;
        assert_eq!(records.len(), 1);
        let records =
            read.get_records_by_collections(&[&Nsid::new("a.a.d".to_string()).unwrap()], 100)?;
        assert_eq!(records.len(), 0);

        Ok(())
    }

    #[test]
    fn test_delete_account() -> anyhow::Result<()> {
        let (read, mut write) = fjall_db();

        let mut batch = TestBatch::default();
        batch.create(
            "did:plc:person-a",
            "a.a.a",
            "rkey-aaa",
            "{}",
            Some("rev-aaa"),
            None,
            10_000,
        );
        for i in 1..=2 {
            batch.create(
                "did:plc:person-b",
                "a.a.a",
                &format!("rkey-bbb-{i}"),
                &format!(r#"{{"n": {i}}}"#),
                Some(&format!("rev-bbb-{i}")),
                None,
                11_000 + i,
            );
        }
        write.insert_batch(batch.batch)?;

        let records =
            read.get_records_by_collections(&[&Nsid::new("a.a.a".to_string()).unwrap()], 100)?;
        assert_eq!(records.len(), 3);

        let records_deleted =
            write.delete_account(&Did::new("did:plc:person-b".to_string()).unwrap())?;
        assert_eq!(records_deleted, 2);

        let records =
            read.get_records_by_collections(&[&Nsid::new("a.a.a".to_string()).unwrap()], 100)?;
        assert_eq!(records.len(), 1);

        Ok(())
    }

    #[test]
    fn rollup_delete_account_removes_record() -> anyhow::Result<()> {
        let (read, mut write) = fjall_db();

        let mut batch = TestBatch::default();
        batch.create(
            "did:plc:person-a",
            "a.a.a",
            "rkey-aaa",
            "{}",
            Some("rev-aaa"),
            None,
            10_000,
        );
        write.insert_batch(batch.batch)?;

        let mut batch = TestBatch::default();
        batch.delete_account("did:plc:person-a", 9_999); // queue it before the rollup
        write.insert_batch(batch.batch)?;

        write.step_rollup()?;

        let records =
            read.get_records_by_collections(&[&Nsid::new("a.a.a".to_string()).unwrap()], 1)?;
        assert_eq!(records.len(), 0);

        Ok(())
    }

    #[test]
    fn rollup_delete_live_count_step() -> anyhow::Result<()> {
        let (read, mut write) = fjall_db();

        let mut batch = TestBatch::default();
        batch.create(
            "did:plc:person-a",
            "a.a.a",
            "rkey-aaa",
            "{}",
            Some("rev-aaa"),
            None,
            10_000,
        );
        write.insert_batch(batch.batch)?;

        let n = write.step_rollup()?;
        assert_eq!(n, 1);

        let mut batch = TestBatch::default();
        batch.delete_account("did:plc:person-a", 10_001);
        write.insert_batch(batch.batch)?;

        let records =
            read.get_records_by_collections(&[&Nsid::new("a.a.a".to_string()).unwrap()], 1)?;
        assert_eq!(records.len(), 1);

        let n = write.step_rollup()?;
        assert_eq!(n, 1);

        let records =
            read.get_records_by_collections(&[&Nsid::new("a.a.a".to_string()).unwrap()], 1)?;
        assert_eq!(records.len(), 0);

        let mut batch = TestBatch::default();
        batch.delete_account("did:plc:person-a", 9_999);
        write.insert_batch(batch.batch)?;

        let n = write.step_rollup()?;
        assert_eq!(n, 0);

        Ok(())
    }

    #[test]
    fn rollup_multiple_count_batches() -> anyhow::Result<()> {
        let (_read, mut write) = fjall_db();

        let mut batch = TestBatch::default();
        batch.create(
            "did:plc:person-a",
            "a.a.a",
            "rkey-aaa",
            "{}",
            Some("rev-aaa"),
            None,
            10_000,
        );
        write.insert_batch(batch.batch)?;

        let mut batch = TestBatch::default();
        batch.create(
            "did:plc:person-a",
            "a.a.a",
            "rkey-aab",
            "{}",
            Some("rev-aab"),
            None,
            10_001,
        );
        write.insert_batch(batch.batch)?;

        let n = write.step_rollup()?;
        assert_eq!(n, 2);

        let n = write.step_rollup()?;
        assert_eq!(n, 0);

        Ok(())
    }

    #[test]
    fn counts_before_and_after_rollup() -> anyhow::Result<()> {
        let (read, mut write) = fjall_db();

        let mut batch = TestBatch::default();
        batch.create(
            "did:plc:person-a",
            "a.a.a",
            "rkey-aaa",
            "{}",
            Some("rev-aaa"),
            None,
            10_000,
        );
        batch.create(
            "did:plc:person-b",
            "a.a.a",
            "rkey-bbb",
            "{}",
            Some("rev-bbb"),
            None,
            10_001,
        );
        write.insert_batch(batch.batch)?;

        let mut batch = TestBatch::default();
        batch.delete_account("did:plc:person-a", 11_000);
        write.insert_batch(batch.batch)?;

        let mut batch = TestBatch::default();
        batch.create(
            "did:plc:person-a",
            "a.a.a",
            "rkey-aac",
            "{}",
            Some("rev-aac"),
            None,
            12_000,
        );
        write.insert_batch(batch.batch)?;

        // before any rollup
        let (records, dids) =
            read.get_counts_by_collection(&Nsid::new("a.a.a".to_string()).unwrap())?;
        assert_eq!(records, 3);
        assert_eq!(dids, 2);

        // first batch rolled up
        let n = write.step_rollup()?;
        assert_eq!(n, 1);

        let (records, dids) =
            read.get_counts_by_collection(&Nsid::new("a.a.a".to_string()).unwrap())?;
        assert_eq!(records, 3);
        assert_eq!(dids, 2);

        // delete account rolled up
        let n = write.step_rollup()?;
        assert_eq!(n, 1);

        let (records, dids) =
            read.get_counts_by_collection(&Nsid::new("a.a.a".to_string()).unwrap())?;
        assert_eq!(records, 3);
        assert_eq!(dids, 2);

        // second batch rolled up
        let n = write.step_rollup()?;
        assert_eq!(n, 1);

        let (records, dids) =
            read.get_counts_by_collection(&Nsid::new("a.a.a".to_string()).unwrap())?;
        assert_eq!(records, 3);
        assert_eq!(dids, 2);

        // no more rollups left
        let n = write.step_rollup()?;
        assert_eq!(n, 0);

        Ok(())
    }
}

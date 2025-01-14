use super::{LinkStorage, StorageBackend};
use anyhow::Result;
use bincode::Options as BincodeOptions;
use link_aggregator::{Did, RecordId};
use links::CollectedLink;
use rocksdb::{
    ColumnFamilyDescriptor, DBWithThreadMode, MergeOperands, MultiThreaded, Options, WriteBatch,
};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

static DID_IDS_CF: &str = "did_ids";
static TARGET_IDS_CF: &str = "target_ids";
static TARGET_LINKERS_CF: &str = "target_links";
static LINK_TARGETS_CF: &str = "link_targets";

static DID_ID_SEQ: AtomicU64 = AtomicU64::new(1); // todo
static TARGET_ID_SEQ: AtomicU64 = AtomicU64::new(1); // todo

// todo: actually understand and set these options probably better
fn rocks_opts_base() -> Options {
    let mut opts = Options::default();
    opts.set_level_compaction_dynamic_level_bytes(true);
    opts.create_if_missing(true);
    opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
    opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Zstd);
    opts
}
fn get_db_opts() -> Options {
    let mut opts = rocks_opts_base();
    opts.create_missing_column_families(true);
    opts
}

#[derive(Debug, Clone)]
pub struct RocksStorage(RocksStorageData);

#[derive(Debug, Clone)]
struct RocksStorageData {
    db: Arc<DBWithThreadMode<MultiThreaded>>,
}

impl RocksStorage {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self(RocksStorageData::new(path)?))
    }
}

impl LinkStorage for RocksStorage {
    fn summarize(&self, qsize: u32) {
        let did_seq = DID_ID_SEQ.load(Ordering::Relaxed);
        let target_seq = TARGET_ID_SEQ.load(Ordering::Relaxed);
        println!("queue: {qsize}. did seq: {did_seq}, target seq: {target_seq}.");
    }
}

trait AsRocksKey {
    fn as_rocks_key(&self) -> &impl Serialize
    where
        Self: Serialize + Sized,
    {
        self
    }
}
trait AsRocksKeyPrefix {
    fn as_rocks_key(&self) -> &impl Serialize
    where
        Self: Serialize + Sized,
    {
        self
    }
}
trait AsRocksValue {
    fn as_rocks_value(&self) -> &impl Serialize
    where
        Self: Serialize + Sized,
    {
        self
    }
}
trait AsRocksMergeOp {
    fn as_rocks_merge_op(&self) -> &impl Serialize
    where
        Self: Serialize + Sized,
    {
        self
    }
}
trait KeyFromRocks<'a>: Deserialize<'a> {}
trait ValueFromRocks<'a>: Deserialize<'a> {}
trait MergeOpFromRocks<'a>: Deserialize<'a> {}

fn _encode_rocks_bytes(o: &impl Serialize) -> Vec<u8> {
    bincode::DefaultOptions::new().serialize(o).unwrap()
}
fn _decode_rocks_bytes<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> Result<T> {
    Ok(bincode::DefaultOptions::new().deserialize(bytes)?)
}

fn _rk(k: impl AsRocksKey + Serialize) -> Vec<u8> {
    _encode_rocks_bytes(k.as_rocks_key())
}
fn _rkp(kp: impl AsRocksKeyPrefix + Serialize) -> Vec<u8> {
    _encode_rocks_bytes(kp.as_rocks_key())
}
fn _rv(v: impl AsRocksValue + Serialize) -> Vec<u8> {
    _encode_rocks_bytes(v.as_rocks_value())
}
fn _rm(m: impl AsRocksMergeOp + Serialize) -> Vec<u8> {
    _encode_rocks_bytes(m.as_rocks_merge_op())
}
fn _kr<'a, T: KeyFromRocks<'a>>(bytes: &'a [u8]) -> Result<T> {
    _decode_rocks_bytes(bytes)
}
fn _vr<'a, T: ValueFromRocks<'a>>(bytes: &'a [u8]) -> Result<T> {
    _decode_rocks_bytes(bytes)
}
fn _mr<'a, T: MergeOpFromRocks<'a>>(bytes: &'a [u8]) -> Result<T> {
    _decode_rocks_bytes(bytes)
}

// did_id table
impl AsRocksKey for &Did {}
impl AsRocksValue for &DidIdValue {}
impl ValueFromRocks<'_> for DidIdValue {}

// target_ids table
impl AsRocksKey for &TargetKey {}
impl AsRocksValue for &TargetId {}
impl ValueFromRocks<'_> for TargetId {}

// target_links table
impl AsRocksKey for &TargetId {}
impl AsRocksValue for &TargetLinkers {}
impl ValueFromRocks<'_> for TargetLinkers {}
impl AsRocksMergeOp for &DidId {}
impl MergeOpFromRocks<'_> for DidId {}

// record_link_targets table
impl AsRocksKey for &RecordLinkKey {}
impl AsRocksKeyPrefix for &RecordLinkKeyDidIdPrefix {} // TODO
impl KeyFromRocks<'_> for RecordLinkKey {}
impl AsRocksValue for &RecordLinkTargets {}
impl ValueFromRocks<'_> for RecordLinkTargets {}

impl RocksStorageData {
    fn new(path: impl AsRef<Path>) -> Result<Self> {
        let db = DBWithThreadMode::open_cf_descriptors(
            &get_db_opts(),
            path,
            vec![
                ColumnFamilyDescriptor::new(DID_IDS_CF, rocks_opts_base()),
                ColumnFamilyDescriptor::new(TARGET_IDS_CF, rocks_opts_base()),
                // the reverse links:
                ColumnFamilyDescriptor::new(TARGET_LINKERS_CF, {
                    let mut opts = rocks_opts_base();
                    opts.set_merge_operator_associative("concat_did_ids", concat_did_ids);
                    opts
                }),
                // unfortunately we also need forward links to handle deletes
                ColumnFamilyDescriptor::new(LINK_TARGETS_CF, rocks_opts_base()),
            ],
        )?;
        Ok(Self { db: Arc::new(db) })
    }

    fn get_did_id_value(&self, did: &Did) -> Result<Option<DidIdValue>> {
        let cf = self.db.cf_handle(DID_IDS_CF).unwrap();
        if let Some(bytes) = self.db.get_cf(&cf, _rk(did))? {
            let did_id_value: DidIdValue = _vr(&bytes)?;
            let current_seq = DID_ID_SEQ.load(Ordering::Relaxed);
            let DidIdValue(DidId(n), _) = did_id_value;
            if n > (current_seq + 10) {
                panic!("found did id greater than current seq: {current_seq}");
            }
            Ok(Some(did_id_value))
        } else {
            Ok(None)
        }
    }
    fn get_or_create_did_id_value(&self, batch: &mut WriteBatch, did: &Did) -> Result<DidIdValue> {
        let cf = self.db.cf_handle(DID_IDS_CF).unwrap();
        Ok(self.get_did_id_value(did)?.unwrap_or_else(|| {
            let did_id = DidId(DID_ID_SEQ.fetch_add(1, Ordering::SeqCst));
            let did_id_value = DidIdValue(did_id, true);
            batch.put_cf(&cf, _rk(did), _rv(&did_id_value));
            // todo: also persist seq
            did_id_value
        }))
    }
    fn update_did_id_value<F>(&self, batch: &mut WriteBatch, did: &Did, update: F) -> Result<bool>
    where
        F: FnOnce(DidIdValue) -> Option<DidIdValue>,
    {
        let cf = self.db.cf_handle(DID_IDS_CF).unwrap();
        let Some(did_id_value) = self.get_did_id_value(did)? else {
            return Ok(false);
        };
        let Some(new_did_id_value) = update(did_id_value) else {
            return Ok(false);
        };
        batch.put_cf(&cf, _rk(did), _rv(&new_did_id_value));
        Ok(true)
    }
    fn delete_did_id_value(&self, batch: &mut WriteBatch, did: &Did) {
        let cf = self.db.cf_handle(DID_IDS_CF).unwrap();
        batch.delete_cf(&cf, _rk(did));
    }

    fn get_target_id(&self, target_key: &TargetKey) -> Result<Option<TargetId>> {
        let cf = self.db.cf_handle(TARGET_IDS_CF).unwrap();
        if let Some(bytes) = self.db.get_cf(&cf, _rk(target_key))? {
            let target_id: TargetId = _vr(&bytes)?;
            let current_seq = TARGET_ID_SEQ.load(Ordering::Relaxed);
            if target_id.0 > (current_seq + 10) {
                panic!("found target id greater than current seq: {current_seq}");
            }
            Ok(Some(target_id))
        } else {
            Ok(None)
        }
    }
    fn get_or_create_target_id(
        &self,
        batch: &mut WriteBatch,
        target_key: &TargetKey,
    ) -> Result<TargetId> {
        let cf = self.db.cf_handle(TARGET_IDS_CF).unwrap();
        Ok(self.get_target_id(target_key)?.unwrap_or_else(|| {
            let target_id = TargetId(TARGET_ID_SEQ.fetch_add(1, Ordering::SeqCst));
            batch.put_cf(&cf, _rk(target_key), _rv(&target_id));
            // todo: also persist seq
            target_id
        }))
    }

    fn get_target_linkers(&self, target_id: &TargetId) -> Result<TargetLinkers> {
        let cf = self.db.cf_handle(TARGET_LINKERS_CF).unwrap();
        let Some(linkers_bytes) = self.db.get_cf(&cf, _rk(target_id))? else {
            return Ok(TargetLinkers::default());
        };
        _vr(&linkers_bytes)
    }
    fn merge_target_linker(
        &self,
        batch: &mut WriteBatch,
        target_id: &TargetId,
        linker_did_id: &DidId,
    ) {
        let cf = self.db.cf_handle(TARGET_LINKERS_CF).unwrap();
        batch.merge_cf(&cf, _rk(target_id), _rm(linker_did_id));
    }
    fn update_target_linkers<F>(
        &self,
        batch: &mut WriteBatch,
        target_id: &TargetId,
        update: F,
    ) -> Result<bool>
    where
        F: FnOnce(TargetLinkers) -> Option<TargetLinkers>,
    {
        let cf = self.db.cf_handle(TARGET_LINKERS_CF).unwrap();
        let existing_linkers = self.get_target_linkers(target_id)?;
        let Some(new_linkers) = update(existing_linkers) else {
            return Ok(false);
        };
        batch.put_cf(&cf, _rk(target_id), _rv(&new_linkers));
        Ok(true)
    }

    fn put_link_targets(
        &self,
        batch: &mut WriteBatch,
        record_link_key: &RecordLinkKey,
        targets: &RecordLinkTargets,
    ) {
        // todo: we are almost idempotent to link creates with this blind write, but we'll still
        // merge in the reverse index. we could read+modify+write here but it'll be SLOWWWWW on
        // the path that we need to be fast. we could go back to a merge op and probably be
        // consistent. or we can accept just a littttttle inconsistency and be idempotent on
        // forward links but not reverse, slightly messing up deletes :/
        // _maybe_ we could run in slow idempotent r-m-w mode during firehose catch-up at the start,
        // then switch to the fast version?
        let cf = self.db.cf_handle(LINK_TARGETS_CF).unwrap();
        batch.put_cf(&cf, _rk(record_link_key), _rv(targets));
    }
    fn get_record_link_targets(
        &self,
        record_link_key: &RecordLinkKey,
    ) -> Result<Option<RecordLinkTargets>> {
        let cf = self.db.cf_handle(LINK_TARGETS_CF).unwrap();
        if let Some(bytes) = self.db.get_cf(&cf, _rk(record_link_key))? {
            Ok(Some(_vr(&bytes)?))
        } else {
            Ok(None)
        }
    }
    fn delete_record_link(&self, batch: &mut WriteBatch, record_link_key: &RecordLinkKey) {
        let cf = self.db.cf_handle(LINK_TARGETS_CF).unwrap();
        batch.delete_cf(&cf, _rk(record_link_key));
    }
}

impl StorageBackend for RocksStorage {
    fn add_links(&self, record_id: &RecordId, links: &[CollectedLink]) {
        let mut batch = WriteBatch::default();

        let DidIdValue(did_id, _) = self
            .0
            .get_or_create_did_id_value(&mut batch, &record_id.did)
            .unwrap();

        let record_link_key = RecordLinkKey(
            did_id,
            Collection(record_id.collection()),
            RKey(record_id.rkey()),
        );
        let mut record_link_targets = RecordLinkTargets::with_capacity(links.len());

        for CollectedLink { target, path } in links {
            let target_key = TargetKey(
                Target(target.clone()),
                Collection(record_id.collection()),
                RPath(path.clone()),
            );
            let target_id = self
                .0
                .get_or_create_target_id(&mut batch, &target_key)
                .unwrap();
            self.0.merge_target_linker(&mut batch, &target_id, &did_id);

            record_link_targets.add(RecordLinkTarget(RPath(path.clone()), target_id))
        }

        self.0
            .put_link_targets(&mut batch, &record_link_key, &record_link_targets);
        self.0.db.write(batch).unwrap();
    }

    fn remove_links(&self, record_id: &RecordId) {
        let mut batch = WriteBatch::default();

        let Some(DidIdValue(linking_did_id, did_active)) =
            self.0.get_did_id_value(&record_id.did).unwrap()
        else {
            return; // we don't know her: nothing to do
        };
        if !did_active {
            eprintln!(
                "removing links from apparently-inactive did {:?}",
                &record_id.did
            );
        }

        let record_link_key = RecordLinkKey(
            linking_did_id,
            Collection(record_id.collection()),
            RKey(record_id.rkey()),
        );
        let Some(record_link_targets) = self.0.get_record_link_targets(&record_link_key).unwrap()
        else {
            return; // we don't have these links
        };

        // we do read -> modify -> write here: could merge-op in the deletes instead?
        // otherwise it's another single-thread-constraining thing.
        for (i, RecordLinkTarget(rpath, target_id)) in record_link_targets.0.iter().enumerate() {
            self.0.update_target_linkers(&mut batch, target_id, |mut linkers| {
                if linkers.0.is_empty() {
                    eprintln!("about to blow up because a linked target is apparently missing.");
                    eprintln!("removing links for: {record_id:?}");
                    eprintln!("found links: {record_link_targets:?}");
                    eprintln!("working on #{i}: {rpath:?} / {target_id:?}");
                    panic!("it was empty");
                }
                if !linkers.remove_last_linker(&linking_did_id) {
                    eprintln!("about to blow up because a linked target apparently does not have us in its dids.");
                    eprintln!("removing links for: {record_id:?}");
                    eprintln!("found links: {record_link_targets:?}");
                    eprintln!("working on #{i}: {rpath:?} / {target_id:?}");
                    eprintln!("trying to find us ({linking_did_id:?}) in {linkers:?}");
                    panic!("reverse index didn't have us");
                }
                Some(linkers)
            }).unwrap();
        }

        self.0.delete_record_link(&mut batch, &record_link_key);
        self.0.db.write(batch).unwrap();
    }

    fn set_account(&self, did: &Did, active: bool) {
        // this needs to be read-modify-write since the did_id needs to stay the same,
        // which has a benefit of allowing to avoid adding entries for dids we don't
        // need. reading on dids needs to be cheap anyway for the current design, and
        // did active/inactive updates are low-freq in the firehose so, eh, it's fine.
        let mut batch = WriteBatch::default();
        self.0
            .update_did_id_value(&mut batch, did, |current_value| {
                if current_value.is_active() == active {
                    eprintln!("set_account: did {did:?} was already set to active={active:?}");
                    return None;
                }
                Some(DidIdValue(current_value.did_id(), active))
            })
            .unwrap();
        self.0.db.write(batch).unwrap();
    }

    fn delete_account(&self, did: &Did) {
        let link_targets_cf = self.0.db.cf_handle(LINK_TARGETS_CF).unwrap();

        let mut batch = WriteBatch::default();

        let Some(DidIdValue(did_id, active)) = self.0.get_did_id_value(did).unwrap() else {
            return; // ignore updates for dids we don't know about
        };
        self.0.delete_did_id_value(&mut batch, did);

        let did_id_prefix = RecordLinkKeyDidIdPrefix(did_id);
        for (i, item) in self
            .0
            .db
            .prefix_iterator_cf(&link_targets_cf, _rkp(&did_id_prefix))
            .enumerate()
        {
            let (key_bytes, fwd_links_bytes) = item.unwrap();
            let record_link_key: RecordLinkKey = _kr(&key_bytes).unwrap();

            self.0.delete_record_link(&mut batch, &record_link_key); // _could_ use delete range here instead of individual deletes, but since we have to scan anyway it's not obvious if it's better

            let links: RecordLinkTargets = _vr(&fwd_links_bytes).unwrap();
            for (j, RecordLinkTarget(path, target_link_id)) in links.0.iter().enumerate() {
                self.0.update_target_linkers(&mut batch, target_link_id, |mut linkers| {
                    if !linkers.remove_last_linker(&did_id) {
                        eprintln!("DELETING ACCOUNT: blowing up: missing linker entry in linked target.");
                        eprintln!("account: {did:?}");
                        eprintln!("did_id: {did_id:?}, was active? {active:?}");
                        eprintln!("with links: {links:?}");
                        eprintln!("working on #{i}.#{j}: {:?} / {path:?} / {target_link_id:?}", record_link_key.collection());
                        eprintln!("but could not find this link :/");
                        panic!("ohnoooo");
                    }
                    eprintln!("managed to delete while deleting {did:?}...");
                    Some(linkers)
                }).unwrap();
            }
        }

        self.0.db.write(batch).unwrap();
    }

    fn count(&self, target: &str, collection: &str, path: &str) -> Result<u64> {
        let target_key = TargetKey(
            Target(target.to_string()),
            Collection(collection.to_string()),
            RPath(path.to_string()),
        );
        if let Some(target_id) = self.0.get_target_id(&target_key)? {
            Ok(self.0.get_target_linkers(&target_id)?.count())
        } else {
            Ok(0)
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Collection(String);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RPath(String);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RKey(String);

// did ids
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
struct DidId(u64);

#[derive(Debug, Serialize, Deserialize)]
struct DidIdValue(DidId, bool); // active or not

impl DidIdValue {
    fn did_id(&self) -> DidId {
        let Self(id, _) = self;
        *id
    }
    fn is_active(&self) -> bool {
        let Self(_, active) = self;
        *active
    }
}

// target ids
#[derive(Debug, Serialize, Deserialize)]
struct TargetId(u64); // key

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Target(String); // the actual target/uri

// targets (uris, dids, etc.): the reverse index
#[derive(Debug, Serialize, Deserialize)]
struct TargetKey(Target, Collection, RPath);

#[derive(Debug, Default, Serialize, Deserialize)]
struct TargetLinkers(Vec<DidId>);

impl TargetLinkers {
    fn remove_last_linker(&mut self, did: &DidId) -> bool {
        if let Some(last_position) = self.0.iter().rposition(|d| d == did) {
            self.0.remove(last_position);
            true
        } else {
            false
        }
    }
    fn count(&self) -> u64 {
        self.0.len() as u64
    }
}

// forward links to targets so we can delete links
#[derive(Debug, Serialize, Deserialize)]
struct RecordLinkKey(DidId, Collection, RKey);

impl RecordLinkKey {
    fn collection(&self) -> Collection {
        let Self(_, collection, _) = self;
        collection.clone()
    }
}

// does this even work????
#[derive(Debug, Serialize, Deserialize)]
struct RecordLinkKeyDidIdPrefix(DidId);

#[derive(Debug, Serialize, Deserialize)]
struct RecordLinkTarget(RPath, TargetId);

#[derive(Debug, Default, Serialize, Deserialize)]
struct RecordLinkTargets(Vec<RecordLinkTarget>);

impl RecordLinkTargets {
    fn with_capacity(cap: usize) -> Self {
        Self(Vec::with_capacity(cap))
    }
    fn add(&mut self, target: RecordLinkTarget) {
        self.0.push(target)
    }
}

fn concat_did_ids(
    _new_key: &[u8],
    existing: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut tls: TargetLinkers = existing
        .map(|existing_bytes| _vr(existing_bytes).unwrap())
        .unwrap_or_default();

    let current_seq = DID_ID_SEQ.load(Ordering::Relaxed);

    for did_id in &tls.0 {
        let DidId(ref n) = did_id;
        if *n > current_seq {
            eprintln!("problem with concat_did_ids. existing: {tls:?}");
            eprintln!(
                "an entry has did_id={n}, which is higher than the current sequence: {current_seq}"
            );
            panic!("got a did to merge with higher-than-current did_id sequence");
        }
    }

    for op in operands {
        let did_id: DidId = _mr(op).unwrap();
        {
            let DidId(ref n) = &did_id;
            if *n > current_seq {
                let orig: Option<TargetLinkers> =
                    existing.map(|existing_bytes| _vr(existing_bytes).unwrap());
                eprintln!("problem with concat_did_ids. existing: {orig:?}\nnew did: {did_id:?}");
                eprintln!("the current sequence is {current_seq}");
                panic!("did_id a did to a number higher than the current sequence");
            }
        }
        tls.0.push(did_id);
    }
    Some(_rv(&tls))
}

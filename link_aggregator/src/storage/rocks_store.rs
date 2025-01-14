use super::{LinkStorage, StorageBackend};
use anyhow::Result;
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

static DID_IDS_CF: &str = "dids";
static TARGET_IDS_CF: &str = "target_ids";
static TARGET_LINKERS_CF: &str = "target_links";
static LINK_TARGETS_CF: &str = "link_targets";

static DID_ID_SEQ: AtomicU64 = AtomicU64::new(1); // todo
static TARGET_ID_SEQ: AtomicU64 = AtomicU64::new(1); // todo

// todo: actually understand and set these options probably better
fn _rocks_opts_base() -> Options {
    let mut opts = Options::default();
    opts.set_level_compaction_dynamic_level_bytes(true);
    opts.create_if_missing(true);
    opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
    opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Zstd);
    opts
}
fn get_db_opts() -> Options {
    let mut opts = _rocks_opts_base();
    opts.create_missing_column_families(true);
    opts
}
fn get_ids_cf_opts() -> Options {
    _rocks_opts_base()
}

#[derive(Debug, Clone)]
pub struct RocksStorage(RocksStorageData);

#[derive(Debug, Clone)]
struct RocksStorageData {
    db: Arc<DBWithThreadMode<MultiThreaded>>,
}

impl RocksStorage {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let db = DBWithThreadMode::open_cf_descriptors(
            &get_db_opts(),
            path,
            vec![
                ColumnFamilyDescriptor::new(DID_IDS_CF, get_ids_cf_opts()),
                ColumnFamilyDescriptor::new(TARGET_IDS_CF, get_ids_cf_opts()),
                ColumnFamilyDescriptor::new(TARGET_LINKERS_CF, {
                    let mut opts = _rocks_opts_base();
                    opts.set_merge_operator_associative("concat_did_ids", concat_did_ids);
                    opts
                }),
                ColumnFamilyDescriptor::new(LINK_TARGETS_CF, {
                    let mut opts = _rocks_opts_base();
                    opts.set_merge_operator_associative("concat_link_targets", concat_link_targets);
                    opts
                }),
            ],
        )?;
        Ok(Self(RocksStorageData {
            db: Arc::new(db),
            // DID_ID_SEQ: Arc::new(AtomicU64::new(1)), // TODO
            // TARGET_ID_SEQ: Arc::new(AtomicU64::new(1)), // TODO
        }))
    }
}

impl LinkStorage for RocksStorage {
    fn summarize(&self, qsize: u32) {
        let did_seq = DID_ID_SEQ.load(Ordering::Relaxed);
        let target_seq = TARGET_ID_SEQ.load(Ordering::Relaxed);
        println!("queue: {qsize}. did seq: {did_seq}, target seq: {target_seq}.");
    }
}

impl RocksStorageData {
    fn get_did_id_value(&self, did: &Did) -> Result<Option<DidIdValue>> {
        let cf = self.db.cf_handle(DID_IDS_CF).unwrap();
        if let Some(bytes) = self.db.get_cf(&cf, did_key(did))? {
            let did_id_value = DidIdValue::from_bytes(&bytes)?;
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
            batch.put_cf(&cf, did_key(did), did_id_value.to_bytes());
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
        batch.put_cf(&cf, did_key(did), new_did_id_value.to_bytes());
        Ok(true)
    }
    fn delete_did_id_value(&self, batch: &mut WriteBatch, did: &Did) {
        let cf = self.db.cf_handle(DID_IDS_CF).unwrap();
        batch.delete_cf(&cf, did_key(did));
    }

    fn get_target_id(&self, target_key: &TargetKey) -> Result<Option<TargetId>> {
        let cf = self.db.cf_handle(TARGET_IDS_CF).unwrap();
        if let Some(bytes) = self.db.get_cf(&cf, target_key.as_key())? {
            let target_id = TargetId::from_bytes(&bytes)?;
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
            batch.put_cf(&cf, target_key.as_key(), target_id.to_bytes());
            // todo: also persist seq
            target_id
        }))
    }
    fn get_target_linkers(&self, target_id: &TargetId) -> Result<TargetLinkers> {
        let cf = self.db.cf_handle(TARGET_LINKERS_CF).unwrap();
        let Some(linkers_bytes) = self.db.get_cf(&cf, target_id.to_bytes())? else {
            return Ok(TargetLinkers::default());
        };
        TargetLinkers::from_bytes(&linkers_bytes)
    }
    fn merge_target_linker(
        &self,
        batch: &mut WriteBatch,
        target_id: &TargetId,
        linker_did_id: &DidId,
    ) {
        let cf = self.db.cf_handle(TARGET_LINKERS_CF).unwrap();
        batch.merge_cf(&cf, target_id.to_bytes(), linker_did_id.linker_bytes());
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
        batch.put_cf(&cf, target_id.to_bytes(), new_linkers.to_bytes());
        Ok(true)
    }
}

impl StorageBackend for RocksStorage {
    fn add_links(&self, record_id: &RecordId, links: &[CollectedLink]) {
        let link_targets_cf = self.0.db.cf_handle(LINK_TARGETS_CF).unwrap();

        // despite all the Arcs there can be only one writer thread
        let mut batch = WriteBatch::default();

        let DidIdValue(did_id, _) = self
            .0
            .get_or_create_did_id_value(&mut batch, &record_id.did)
            .unwrap();

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
            let fwd_link_key = bincode::serialize(&LinkKey(
                did_id,
                Collection(record_id.collection()),
                RKey(record_id.rkey()),
            ))
            .unwrap();
            let link_target_bytes =
                bincode::serialize(&LinkTarget(RPath(path.clone()), target_id)).unwrap();
            batch.merge_cf(&link_targets_cf, &fwd_link_key, &link_target_bytes);
        }
        self.0.db.write(batch).unwrap();
    }

    fn remove_links(&self, record_id: &RecordId) {
        let link_targets_cf = self.0.db.cf_handle(LINK_TARGETS_CF).unwrap();

        // despite all the Arcs there can be only one writer thread
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

        let fwd_link_key = bincode::serialize(&LinkKey(
            linking_did_id,
            Collection(record_id.collection()),
            RKey(record_id.rkey()),
        ))
        .unwrap();

        let Some(links_bytes) = self.0.db.get_cf(&link_targets_cf, &fwd_link_key).unwrap() else {
            return; // we don't have these links
        };
        let links = RecordLinkTargets::from_bytes(&links_bytes).unwrap();

        // we do read -> modify -> write here: could merge-op in the deletes instead?
        // otherwise it's another single-thread-constraining thing.
        for (i, LinkTarget(rpath, target_id)) in links.0.iter().enumerate() {
            self.0.update_target_linkers(&mut batch, target_id, |mut linkers| {
                if linkers.0.is_empty() {
                    eprintln!("about to blow up because a linked target is apparently missing.");
                    eprintln!("removing links for: {record_id:?}");
                    eprintln!("found links: {links:?}");
                    eprintln!("from links bytes: {links_bytes:?}");
                    eprintln!("working on #{i}: {rpath:?} / {target_id:?}");
                    return None
                }
                if !linkers.remove_last_linker(&linking_did_id) {
                    eprintln!("about to blow up because a linked target apparently does not have us in its dids.");
                    eprintln!("removing links for: {record_id:?}");
                    eprintln!("found links: {links:?}");
                    eprintln!("working on #{i}: {rpath:?} / {target_id:?}");
                    eprintln!("trying to find us ({linking_did_id:?}) in {linkers:?}");
                    return None
                }
                Some(linkers)
            }).unwrap();
        }

        batch.delete_cf(&link_targets_cf, &fwd_link_key);

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

        // TODO: relying on bincode to serialize to working prefix bytes is probably not wise.
        let did_id_prefix = LinkKeyDidIdPrefix(did_id);
        let did_id_prefix_bytes = bincode::serialize(&did_id_prefix).unwrap();
        for (i, item) in self
            .0
            .db
            .prefix_iterator_cf(&link_targets_cf, &did_id_prefix_bytes)
            .enumerate()
        {
            let (key_bytes, fwd_links_bytes) = item.unwrap();
            // deletes the did forwrd link
            batch.delete_cf(&link_targets_cf, &key_bytes); // not using delete_range here since we have to scan & read already anyway (should we though?)

            let links = RecordLinkTargets::from_bytes(&fwd_links_bytes).unwrap();
            for (j, LinkTarget(path, target_link_id)) in links.0.iter().enumerate() {
                self.0.update_target_linkers(&mut batch, target_link_id, |mut linkers| {
                    if !linkers.remove_last_linker(&did_id) {
                        eprintln!("DELETING ACCOUNT: blowing up: missing linker entry in linked target.");
                        eprintln!("account: {did:?}");
                        eprintln!("did_id: {did_id:?}, was active? {active:?}");
                        eprintln!("with links: {links:?}");
                        eprintln!("working on #{i}.#{j}: {path:?} / {target_link_id:?}");
                        eprintln!("but could not find this link :/");
                        return None
                    }
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

#[derive(Debug, Serialize, Deserialize)]
struct Collection(String);

#[derive(Debug, Serialize, Deserialize)]
struct RPath(String);

#[derive(Debug, Serialize, Deserialize)]
struct RKey(String);

// did ids
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
struct DidId(u64);

impl DidId {
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(bincode::deserialize(bytes)?)
    }
    fn linker_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }
}

fn did_key(did: &Did) -> Vec<u8> {
    bincode::serialize(did).unwrap()
}

#[derive(Debug, Serialize, Deserialize)]
struct DidIdValue(DidId, bool); // active or not

impl DidIdValue {
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(bincode::deserialize(bytes)?)
    }
    fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }
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

impl TargetId {
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(bincode::deserialize(bytes)?)
    }
    fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Target(String); // value

// targets (uris, dids, etc.): the reverse index
#[derive(Debug, Serialize, Deserialize)]
struct TargetKey(Target, Collection, RPath);

impl TargetKey {
    fn as_key(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct TargetLinkers(Vec<DidId>);

impl TargetLinkers {
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(bincode::deserialize(bytes)?)
    }
    fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }
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
struct LinkKey(DidId, Collection, RKey);

// does this even work????
#[derive(Debug, Serialize, Deserialize)]
struct LinkKeyDidIdPrefix(DidId);

#[derive(Debug, Serialize, Deserialize)]
struct LinkTarget(RPath, TargetId);

#[derive(Debug, Default, Serialize, Deserialize)]
struct RecordLinkTargets(Vec<LinkTarget>);

impl RecordLinkTargets {
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(bincode::deserialize(bytes)?)
    }
    fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }
}

fn concat_did_ids(
    _new_key: &[u8],
    existing: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut tls = existing
        .map(|existing_bytes| TargetLinkers::from_bytes(existing_bytes).unwrap())
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
        let did_id = DidId::from_bytes(op).unwrap();
        {
            let DidId(ref n) = &did_id;
            if *n > current_seq {
                let orig: Option<TargetLinkers> = existing
                    .map(|existing_bytes| TargetLinkers::from_bytes(existing_bytes).unwrap());
                eprintln!("problem with concat_did_ids. existing: {orig:?}\nnew did: {did_id:?}");
                eprintln!("the current sequence is {current_seq}");
                panic!("did_id a did to a number higher than the current sequence");
            }
        }
        tls.0.push(did_id);
    }
    Some(tls.to_bytes())
}

fn concat_link_targets(
    _new_key: &[u8],
    existing: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut rlts: RecordLinkTargets = existing
        .map(|existing_bytes| RecordLinkTargets::from_bytes(existing_bytes).unwrap())
        .unwrap_or_default();

    let current_seq = TARGET_ID_SEQ.load(Ordering::Relaxed);

    for link_target in &rlts.0 {
        let LinkTarget(_path, TargetId(ref target_id)) = link_target;
        if *target_id > (current_seq + 10) {
            eprintln!("problem with concat_link_targets. deserialized existing target_id {target_id} higher than current sequence {current_seq}.");
            eprintln!("the full set is {rlts:?}");
            panic!("booo");
        }
    }

    for op in operands {
        let decoded: LinkTarget = bincode::deserialize(op).unwrap();
        {
            let LinkTarget(_, TargetId(ref target_id)) = &decoded;
            if *target_id > (current_seq + 10) {
                let orig: Option<RecordLinkTargets> = existing
                    .map(|existing_bytes| RecordLinkTargets::from_bytes(existing_bytes).unwrap());
                eprintln!("problem with concat_link_targets");
                eprintln!("decoded {decoded:?} with target id {target_id} greater than current seq {current_seq}");
                eprintln!("orig was {orig:?}\nwith orig bytes: {existing:?}");
                eprintln!("this was from bytes {op:?}");
                let ops = operands.iter().collect::<Vec<_>>();
                eprintln!("from operands {ops:?}");
                panic!("ohnoooooo");
            }
        }
        rlts.0.push(decoded);
    }
    Some(rlts.to_bytes())
}

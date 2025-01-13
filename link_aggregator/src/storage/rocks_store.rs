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
    did_id_seq: Arc<AtomicU64>,
    target_id_seq: Arc<AtomicU64>,
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
                    opts.set_merge_operator_associative("concat_dids", concat_dids);
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
            did_id_seq: Arc::new(AtomicU64::new(1)), // TODO
            target_id_seq: Arc::new(AtomicU64::new(1)), // TODO
        }))
    }
}

impl LinkStorage for RocksStorage {} // defaults are fine

impl StorageBackend for RocksStorage {
    fn add_links(&self, record_id: &RecordId, links: &[CollectedLink]) {
        let did_ids_cf = self.0.db.cf_handle(DID_IDS_CF).unwrap();
        let target_ids_cf = self.0.db.cf_handle(TARGET_IDS_CF).unwrap();
        let target_linkers_cf = self.0.db.cf_handle(TARGET_LINKERS_CF).unwrap();
        let link_targets_cf = self.0.db.cf_handle(LINK_TARGETS_CF).unwrap();

        // despite all the Arcs there can be only one writer thread
        let mut batch = WriteBatch::default();

        let actual_linking_did = bincode::serialize(&record_id.did).unwrap();
        let (linking_did_bytes, linking_did) = self
            .0
            .db
            .get_cf(&did_ids_cf, &actual_linking_did)
            .unwrap()
            .map(|did_value_bytes| {
                let DidIdValue(id_typed, active) = bincode::deserialize(&did_value_bytes).unwrap();
                if !active {
                    eprintln!(
                        "adding links for apparently-inactive did {:?}",
                        &record_id.did
                    );
                }
                let id_bytes = bincode::serialize(&id_typed).unwrap();
                (id_bytes, id_typed)
            })
            .unwrap_or_else(|| {
                let id_typed = DidID(self.0.did_id_seq.fetch_add(1, Ordering::SeqCst));
                let id_bytes = bincode::serialize(&id_typed).unwrap();
                let did_value_bytes = bincode::serialize(&DidIdValue(id_typed, true)).unwrap();
                batch.put_cf(&did_ids_cf, &actual_linking_did, &did_value_bytes);
                (id_bytes, id_typed)
            });

        for CollectedLink { target, path } in links {
            let target_key = TargetKey(
                Target(target.clone()),
                Collection(record_id.collection()),
                RPath(path.clone()),
            );
            let actual_target = bincode::serialize(&target_key).unwrap();
            let (target_id_bytes, target_id) = self
                .0
                .db
                .get_cf(&target_ids_cf, &actual_target)
                .unwrap()
                .map(|id_bytes| {
                    let id_typed: TargetID = bincode::deserialize(&id_bytes).unwrap();
                    (id_bytes, id_typed)
                })
                .unwrap_or_else(|| {
                    let id_typed = self.0.target_id_seq.fetch_add(1, Ordering::SeqCst);
                    let id_bytes = bincode::serialize(&id_typed).unwrap();
                    batch.put_cf(&target_ids_cf, &actual_target, &id_bytes);
                    (id_bytes, TargetID(id_typed))
                });

            batch.merge_cf(&target_linkers_cf, &target_id_bytes, &linking_did_bytes);
            let fwd_link_key = bincode::serialize(&LinkKey(
                linking_did,
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
        let did_ids_cf = self.0.db.cf_handle(DID_IDS_CF).unwrap();
        let target_linkers_cf = self.0.db.cf_handle(TARGET_LINKERS_CF).unwrap();
        let link_targets_cf = self.0.db.cf_handle(LINK_TARGETS_CF).unwrap();

        // despite all the Arcs there can be only one writer thread
        let mut batch = WriteBatch::default();

        let actual_linking_did = bincode::serialize(&record_id.did).unwrap();
        let Some(DidIdValue(linking_did_id, did_active)) = self
            .0
            .db
            .get_cf(&did_ids_cf, &actual_linking_did)
            .unwrap()
            .map(|id_value_bytes| bincode::deserialize(&id_value_bytes).unwrap())
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

        let Some(links) = self.0.db.get_cf(&link_targets_cf, &fwd_link_key).unwrap() else {
            return; // we don't have these links
        };
        let links: Vec<LinkTarget> = bincode::deserialize(&links).unwrap();

        // we do read -> modify -> write here: could merge-op in the deletes instead?
        // otherwise it's another single-thread-constraining thing.
        for LinkTarget(_rpath, target_id) in links {
            let target_id_bytes = bincode::serialize(&target_id).unwrap();
            let dids_bytes = self
                .0
                .db
                .get_cf(&target_linkers_cf, &target_id_bytes)
                .unwrap()
                .expect("linked target should exist");
            let mut dids: Vec<DidID> = bincode::deserialize(&dids_bytes).unwrap();
            let last_did_position = dids
                .iter()
                .rposition(|d| *d == linking_did_id)
                .expect("must be in dids list if we have a link to it");
            dids.remove(last_did_position);
            let dids_bytes = bincode::serialize(&dids).unwrap();
            batch.put_cf(&target_linkers_cf, &target_id_bytes, &dids_bytes);
        }

        batch.delete_cf(&link_targets_cf, &fwd_link_key);

        self.0.db.write(batch).unwrap();
    }

    fn set_account(&self, did: &Did, active: bool) {
        // this needs to be read-modify-write since the did_id needs to stay the same,
        // which has a benefit of allowing to avoid adding entries for dids we don't
        // need. reading on dids needs to be cheap anyway for the current design, and
        // did active/inactive updates are low-freq in the firehose so, eh, it's fine.

        let did_ids_cf = self.0.db.cf_handle(DID_IDS_CF).unwrap();
        let did_bytes = bincode::serialize(&did).unwrap();
        let Some(did_value_bytes) = self.0.db.get_cf(&did_ids_cf, &did_bytes).unwrap() else {
            return; // ignore updates for dids we don't know about
        };
        let DidIdValue(did_id, was_active) = bincode::deserialize(&did_value_bytes).unwrap();
        if was_active == active {
            eprintln!(
                "set_account: did {did:?} was already set to active={active:?}, ignoring update"
            );
            return;
        }
        let updated_did_id_value = bincode::serialize(&DidIdValue(did_id, active)).unwrap();
        self.0
            .db
            .put_cf(&did_ids_cf, &did_bytes, &updated_did_id_value)
            .unwrap();
    }

    fn delete_account(&self, did: &Did) {
        let did_ids_cf = self.0.db.cf_handle(DID_IDS_CF).unwrap();
        let target_linkers_cf = self.0.db.cf_handle(TARGET_LINKERS_CF).unwrap();
        let link_targets_cf = self.0.db.cf_handle(LINK_TARGETS_CF).unwrap();

        let mut batch = WriteBatch::default();

        let did_bytes = bincode::serialize(&did).unwrap();
        let Some(did_value_bytes) = self.0.db.get_cf(&did_ids_cf, &did_bytes).unwrap() else {
            return; // ignore updates for dids we don't know about
        };
        batch.delete_cf(&did_ids_cf, &did_value_bytes);

        let DidIdValue(did_id_typed, _active) = bincode::deserialize(&did_value_bytes).unwrap();

        // TODO: relying on bincode to serialize to working prefix bytes is probably not wise.
        let did_id_prefix = LinkKeyDidIdPrefix(did_id_typed);
        let did_id_prefix_bytes = bincode::serialize(&did_id_prefix).unwrap();
        for item in self
            .0
            .db
            .prefix_iterator_cf(&link_targets_cf, &did_id_prefix_bytes)
        {
            let (key_bytes, fwd_links_bytes) = item.unwrap();
            batch.delete_cf(&link_targets_cf, &key_bytes); // not using delete_range here since we have to scan & read already anyway (should we though?)

            let links: Vec<LinkTarget> = bincode::deserialize(&fwd_links_bytes).unwrap();
            for LinkTarget(_path, target_link_id) in links {
                let target_link_id_bytes = bincode::serialize(&target_link_id).unwrap();
                let target_linkers_bytes = self
                    .0
                    .db
                    .get_cf(&target_linkers_cf, &target_link_id_bytes)
                    .unwrap()
                    .expect("linked target should exist");
                let mut target_linkers: Vec<DidID> =
                    bincode::deserialize(&target_linkers_bytes).unwrap();
                target_linkers.retain(|d| *d != did_id_typed);
                let target_linkers_updated_bytes = bincode::serialize(&target_linkers).unwrap();
                batch.put_cf(
                    &target_linkers_cf,
                    &target_link_id_bytes,
                    &target_linkers_updated_bytes,
                );
            }
        }

        self.0.db.write(batch).unwrap();
    }

    fn count(&self, target: &str, collection: &str, path: &str) -> Result<u64> {
        let target_ids_cf = self.0.db.cf_handle(TARGET_IDS_CF).unwrap();
        let target_linkers_cf = self.0.db.cf_handle(TARGET_LINKERS_CF).unwrap();

        let target_key_z = TargetKey(
            Target(target.to_string()),
            Collection(collection.to_string()),
            RPath(path.to_string()),
        );
        let target_key = bincode::serialize(&target_key_z).unwrap();

        if let Some(target_id) = self.0.db.get_cf(&target_ids_cf, &target_key).unwrap() {
            let linkers = self
                .0
                .db
                .get_cf(&target_linkers_cf, target_id)
                .unwrap()
                .expect("target to exist if target id exists");
            let linkers: Vec<DidID> = bincode::deserialize(&linkers).unwrap();
            Ok(linkers.len() as u64)
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
struct DidID(u64);

#[derive(Debug, Serialize, Deserialize)]
struct DidIdValue(DidID, bool); // active or not

// target ids
#[derive(Debug, Serialize, Deserialize)]
struct TargetID(u64); // key

#[derive(Debug, Serialize, Deserialize)]
struct Target(String); // value

// targets (uris, dids, etc.): the reverse index
#[derive(Debug, Serialize, Deserialize)]
struct TargetKey(Target, Collection, RPath);

// target linker is just Did

// forward links to targets so we can delete links
#[derive(Debug, Serialize, Deserialize)]
struct LinkKey(DidID, Collection, RKey);

// does this even work????
#[derive(Debug, Serialize, Deserialize)]
struct LinkKeyDidIdPrefix(DidID);

#[derive(Debug, Serialize, Deserialize)]
struct LinkTarget(RPath, TargetID);

fn concat_dids(
    _new_key: &[u8],
    existing: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut ts: Vec<DidID> = existing
        .map(|existing_bytes| bincode::deserialize(existing_bytes).unwrap())
        .unwrap_or_default();
    for op in operands {
        let decoded: DidID = bincode::deserialize(op).unwrap();
        ts.push(decoded);
    }
    Some(bincode::serialize(&ts).unwrap())
}

fn concat_link_targets(
    _new_key: &[u8],
    existing: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut ts: Vec<LinkTarget> = existing
        .map(|existing_bytes| bincode::deserialize(existing_bytes).unwrap())
        .unwrap_or_default();
    for op in operands {
        let decoded: LinkTarget = bincode::deserialize(op).unwrap();
        ts.push(decoded);
    }
    Some(bincode::serialize(&ts).unwrap())
}

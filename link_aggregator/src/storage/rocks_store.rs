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
        let (linking_did_id, raw_linking_did_id) = self
            .0
            .db
            .get_cf(&did_ids_cf, &actual_linking_did)
            .unwrap()
            .map(|linking_did_id| {
                let id_raw: DidID = bincode::deserialize(&linking_did_id).unwrap();
                (linking_did_id, id_raw)
            })
            .unwrap_or_else(|| {
                let id_raw = self.0.did_id_seq.fetch_add(1, Ordering::SeqCst) + 1;
                let id = bincode::serialize(&id_raw).unwrap();
                batch.put_cf(&did_ids_cf, &actual_linking_did, &id);
                (id, DidID(id_raw))
            });

        for CollectedLink { target, path } in links {
            let target_key = TargetKey(
                Target(target.clone()),
                Collection(record_id.collection()),
                RPath(path.clone()),
            );
            let actual_target = bincode::serialize(&target_key).unwrap();
            let target_id = self
                .0
                .db
                .get_cf(&target_ids_cf, &actual_target)
                .unwrap()
                .unwrap_or_else(|| {
                    let id = self.0.target_id_seq.fetch_add(1, Ordering::SeqCst) + 1;
                    let id = bincode::serialize(&id).unwrap();
                    batch.put_cf(&target_ids_cf, &actual_target, &id);
                    id
                });

            batch.merge_cf(&target_linkers_cf, &target_id, &linking_did_id);
            let fwd_link_key = bincode::serialize(&LinkKey(
                raw_linking_did_id,
                Collection(record_id.collection()),
                RKey(record_id.rkey()),
            ))
            .unwrap();
            batch.merge_cf(&link_targets_cf, &fwd_link_key, &target_id);
        }
        self.0.db.write(batch).unwrap();
    }

    fn set_account(&self, _did: &Did, _active: bool) {
        todo!()
    }

    fn remove_links(&self, _record_id: &RecordId) {
        todo!()
    }

    fn delete_account(&self, _did: &Did) {
        todo!()
    }

    fn count(&self, target: &str, collection: &str, path: &str) -> Result<u64> {
        let did_ids_cf = self.0.db.cf_handle(DID_IDS_CF).unwrap();
        let target_linkers_cf = self.0.db.cf_handle(TARGET_LINKERS_CF).unwrap();

        let target_key = TargetKey(
            Target(target.to_string()),
            Collection(collection.to_string()),
            RPath(path.to_string()),
        );
        let target_key = bincode::serialize(&target_key).unwrap();

        if let Some(target_id) = self.0.db.get_cf(&did_ids_cf, &target_key).unwrap() {
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

#[derive(Serialize, Deserialize)]
struct Collection(String);

#[derive(Serialize, Deserialize)]
struct RPath(String);

#[derive(Serialize, Deserialize)]
struct RKey(String);

// did ids
#[derive(Clone, Copy, Serialize, Deserialize)]
struct DidID(u64); // key

// struct DidValue(Did, bool); // active or not

// target ids
#[derive(Serialize, Deserialize)]
struct TargetID(u64); // key

#[derive(Serialize, Deserialize)]
struct Target(String); // value

// targets (uris, dids, etc.): the reverse index
#[derive(Serialize, Deserialize)]
struct TargetKey(Target, Collection, RPath);

// target linker is just Did

// forward links to targets so we can delete links
#[derive(Serialize, Deserialize)]
struct LinkKey(DidID, Collection, RKey);

#[derive(Serialize, Deserialize)]
struct LinkTarget(RPath, TargetID);

fn concat_dids(
    _new_key: &[u8],
    existing: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut ts = existing
        .map(|existing_bytes| {
            let decoded: Vec<Did> = bincode::deserialize(existing_bytes).unwrap();
            decoded
        })
        .unwrap_or_default();
    for op in operands {
        let decoded: Did = bincode::deserialize(op).unwrap();
        ts.push(decoded);
    }
    Some(bincode::serialize(&ts).unwrap())
}

fn concat_link_targets(
    _new_key: &[u8],
    existing: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut ts = existing
        .map(|existing_bytes| {
            let decoded: Vec<LinkTarget> = bincode::deserialize(existing_bytes).unwrap();
            decoded
        })
        .unwrap_or_default();
    for op in operands {
        let decoded: LinkTarget = bincode::deserialize(op).unwrap();
        ts.push(decoded);
    }
    Some(bincode::serialize(&ts).unwrap())
}

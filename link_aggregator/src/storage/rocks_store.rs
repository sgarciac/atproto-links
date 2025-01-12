use super::{LinkStorage, StorageBackend};
use anyhow::Result;
use link_aggregator::{Did, RecordId};
use links::CollectedLink;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::path::Path;
use std::sync::Arc;

// hopefully-correct simple hashmap version, intended only for tests to verify disk impl
#[derive(Debug, Clone)]
pub struct RocksStorage(RocksStorageData);

#[derive(Debug, Clone)]
struct RocksStorageData {
    db: Arc<DBWithThreadMode<MultiThreaded>>,
}

impl RocksStorage {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let db = DBWithThreadMode::open_default(path)?;
        Ok(Self(RocksStorageData { db: Arc::new(db) }))
    }
}

impl LinkStorage for RocksStorage {} // defaults are fine

impl StorageBackend for RocksStorage {
    fn add_links(&self, _record_id: &RecordId, _links: &[CollectedLink]) {
        todo!()
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

    fn count(&self, _target: &str, _collection: &str, _path: &str) -> Result<u64> {
        todo!()
    }
}

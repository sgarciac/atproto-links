use super::{LinkStorage, StorageBackend};
use anyhow::Result;
use link_aggregator::{Did, RecordId};
use links::CollectedLink;

// hopefully-correct simple hashmap version, intended only for tests to verify disk impl
#[derive(Debug, Clone)]
pub struct RocksStorage(RocksStorageData);

#[derive(Debug, Clone)]
struct RocksStorageData {}

impl RocksStorage {
    pub fn new() -> Self {
        Self(RocksStorageData {})
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

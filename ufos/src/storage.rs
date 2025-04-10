// use crate::store_types::CountsValue;
use crate::{error::StorageError, ConsumerInfo, Cursor, EventBatch, TopCollections, UFOsRecord};
use jetstream::exports::{Did, Nsid};
use std::path::Path;
use async_trait::async_trait;

pub type StorageResult<T> = Result<T, StorageError>;

pub trait StorageWhatever<R: StoreReader, W: StoreWriter, C> {
    fn init(
        path: impl AsRef<Path>,
        endpoint: String,
        force_endpoint: bool,
        config: C,
    ) -> StorageResult<(R, W, Option<Cursor>)>
    where
        Self: Sized;
}

pub trait StoreWriter {
    fn insert_batch<const LIMIT: usize>(
        &mut self,
        event_batch: EventBatch<LIMIT>,
    ) -> StorageResult<()>;

    fn trim_collection(&mut self, collection: &Nsid, limit: usize) -> StorageResult<()>;

    fn delete_account(&mut self, did: &Did) -> StorageResult<usize>;
}

#[async_trait]
pub trait StoreReader: Send + Sync {
    fn get_storage_stats(&self) -> StorageResult<serde_json::Value>;
    async fn get_storage_stats_a(&self) -> StorageResult<serde_json::Value>;

    fn get_consumer_info(&self) -> StorageResult<ConsumerInfo>;

    fn get_top_collections(&self) -> StorageResult<TopCollections>;

    fn get_counts_by_collection(&self, collection: &Nsid) -> StorageResult<(u64, u64)>;

    fn get_records_by_collections(
        &self,
        collections: &[&Nsid],
        limit: usize,
    ) -> StorageResult<Vec<UFOsRecord>>;
}

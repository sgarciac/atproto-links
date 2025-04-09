// use crate::store_types::CountsValue;
use crate::{error::StorageError, ConsumerInfo, Cursor, EventBatch, UFOsRecord};
use jetstream::exports::{Did, Nsid};
use schemars::JsonSchema;
use serde::Serialize;
use std::path::Path;

pub type StorageResult<T> = Result<T, StorageError>;

pub trait StorageWhatever<R, W, C, S>
where
    R: StoreReader<S>,
    W: StoreWriter,
    S: Serialize + JsonSchema,
{
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

pub trait StoreReader<S>: Clone {
    fn get_storage_stats(&self) -> StorageResult<S>;

    fn get_consumer_info(&self) -> StorageResult<ConsumerInfo>;

    fn get_counts_by_collection(&self, collection: &Nsid) -> StorageResult<(u64, u64)>;

    fn get_records_by_collections(
        &self,
        collections: &[&Nsid],
        limit: usize,
    ) -> StorageResult<Vec<UFOsRecord>>;
}

// use crate::store_types::CountsValue;
use crate::{error::StorageError, Cursor, EventBatch, UFOsRecord};
use jetstream::exports::{Did, Nsid};
use std::path::Path;

pub type StorageResult<T> = Result<T, StorageError>;

// #[derive(Debug)]
// pub enum RollupTask {
//     CollectionRollup {
//         live_counts_key_bytes: Vec<u8>,
//         counts: CountsValue,
//     },
//     DeleteAccount(Did),
// }

pub trait StorageWhatever<R: StoreReader, W: StoreWriter, C> {
    // TODO: extract this
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

    // fn rollup_tasks(
    //     &mut self,
    //     from_cursor: Cursor,
    // ) -> impl Iterator<Item = StorageResult<(RollupTask, Cursor)>>;
}

pub trait StoreReader: Clone {
    fn get_counts_by_collection(&self, collection: &Nsid) -> StorageResult<(u64, u64)>;

    fn get_records_by_collections(
        &self,
        collections: &[&Nsid],
        limit: usize,
    ) -> StorageResult<Vec<UFOsRecord>>;
}

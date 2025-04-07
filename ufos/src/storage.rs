use crate::{error::StorageError, Cursor, EventBatch};
use jetstream::exports::Nsid;
use std::path::Path;

pub trait StorageWhatever<R: StoreReader, W: StoreWriter, C> {
    // TODO: extract this
    fn init(
        path: impl AsRef<Path>,
        endpoint: String,
        force_endpoint: bool,
        config: C,
    ) -> Result<(R, W, Option<Cursor>), StorageError>
    where
        Self: Sized;
}

pub trait StoreWriter {
    fn insert_batch(&mut self, event_batch: EventBatch) -> Result<(), StorageError>;
}

pub trait StoreReader: Clone {
    fn get_total_by_collection(&self, collection: &Nsid) -> Result<u64, StorageError>;
}

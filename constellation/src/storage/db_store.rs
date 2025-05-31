use super::AtprotoProcessor;
use crate::{ActionableEvent, Did, RecordId};
use anyhow::Result;
use diesel::r2d2::Pool;
use links::CollectedLink;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

// hopefully-correct simple hashmap version, intended only for tests to verify disk impl
#[derive(Debug, Clone)]
pub struct DbStorage(Arc<Mutex<DbStorageData>>);

type Linkers = Vec<Option<(Did, RKey)>>; // optional because we replace with None for deleted links to keep cursors stable

#[derive(Debug, Default)]
struct DbStorageData {
    pool: Pool<ConnectionManager<PgConnection>>,
}

impl DbStorage {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(DbStorageData::default())))
    }
}

impl Default for MemStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl AtprotoProcessor for MemStorage {
    fn push(&mut self, _event: &ActionableEvent, cursor: u64) -> Result<()> {
        info!("pushing event: {:?}", cursor);
        Ok(())
    }
}

#[derive(Debug, PartialEq, Hash, Eq, Clone)]
struct Target(String);

impl Target {
    fn new(t: &str) -> Self {
        Self(t.into())
    }
}

#[derive(Debug, PartialEq, Hash, Eq, Clone)]
struct Source {
    collection: String,
    path: String,
}

impl Source {
    fn new(collection: &str, path: &str) -> Self {
        Self {
            collection: collection.into(),
            path: path.into(),
        }
    }
}

#[derive(Debug, PartialEq, Hash, Eq, Clone)]
struct RKey(String);

#[derive(Debug, PartialEq, Hash, Eq, Clone)]
struct RepoId {
    collection: String,
    rkey: RKey,
}

impl RepoId {
    fn from_record_id(record_id: &RecordId) -> Self {
        Self {
            collection: record_id.collection.clone(),
            rkey: RKey(record_id.rkey.clone()),
        }
    }
}

#[derive(Debug, PartialEq, Hash, Eq, Clone)]
struct RecordPath(String);

impl RecordPath {
    fn new(rp: &str) -> Self {
        Self(rp.into())
    }
}

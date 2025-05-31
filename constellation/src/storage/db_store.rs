use super::AtprotoProcessor;
use crate::{ActionableEvent, Did, RecordId};
use anyhow::Result;
use diesel::r2d2::{ConnectionManager, Pool};
use diesel::PgConnection;
use std::sync::{Arc, Mutex};
use tracing::info;

// hopefully-correct simple hashmap version, intended only for tests to verify disk impl
#[derive(Debug, Clone)]
pub struct DbStorage(Arc<Mutex<Pool<ConnectionManager<PgConnection>>>>);

impl Default for DbStorage {
    fn default() -> Self {
        Self::new(&std::env::var("DATABASE_URL").expect("DATABASE_URL must be set"))
    }
}

impl DbStorage {
    pub fn new(database_url: &str) -> Self {
        Self(Arc::new(Mutex::new(
            Pool::builder()
                .build(ConnectionManager::new(database_url))
                .unwrap(),
        )))
    }
}

impl AtprotoProcessor for DbStorage {
    fn push(&mut self, _event: &ActionableEvent, cursor: u64) -> Result<()> {
        info!("pushing event: {:?}", cursor);
        //let mut conn_result = self.0.lock().unwrap().get()

        Ok(())
    }
}

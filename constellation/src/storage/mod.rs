use crate::ActionableEvent;
use anyhow::Result;

pub mod mem_store;
pub use mem_store::MemStorage;
pub mod db_store;
pub use db_store::DbStorage;

#[derive(Debug, PartialEq)]
pub struct PagedAppendingCollection<T> {
    pub version: (u64, u64), // (collection length, deleted item count) // TODO: change to (total, active)? since dedups isn't "deleted"
    pub items: Vec<T>,
    pub next: Option<u64>,
}

pub trait AtprotoProcessor: Send + Sync {
    /// jetstream cursor from last saved actions, if available
    async fn get_cursor(&mut self) -> Result<Option<u64>> {
        Ok(None)
    }

    async fn push(&mut self, event: &ActionableEvent, cursor: u64) -> Result<()>;
}

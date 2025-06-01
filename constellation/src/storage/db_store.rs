use super::AtprotoProcessor;
use crate::ActionableEvent;
use anyhow::Result;
use diesel_async::pooled_connection::bb8::Pool;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::AsyncPgConnection;
use tracing::info;

// hopefully-correct simple hashmap version, intended only for tests to verify disk impl
#[derive(Debug, Clone)]
pub struct DbStorage(Pool<AsyncPgConnection>);

impl DbStorage {
    pub async fn new(database_url: &str) -> Self {
        let config =
            AsyncDieselConnectionManager::<diesel_async::AsyncPgConnection>::new(database_url);
        let pool = Pool::builder().build(config).await.unwrap();
        Self(pool)
    }
}

impl AtprotoProcessor for DbStorage {
    async fn push(&mut self, _event: &ActionableEvent, cursor: u64) -> Result<()> {
        info!("pushing event: {:?}", cursor);
        Ok(())
    }
}

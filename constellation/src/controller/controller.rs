// The main orchestrator
use crate::{
    controller::{did_cache, DidCache},
    models::{self, BigintKeyval},
    schema::bigint_keyvals,
    ActionableEvent,
};
use anyhow::Result;
use diesel::prelude::*;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::{pooled_connection::bb8::Pool, AsyncConnection};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info};

#[derive(Clone)]
pub struct Controller {
    connection: Arc<Mutex<AsyncPgConnection>>,
    did_cache: DidCache,
}

// Using a Pool:
// #[derive(Debug, Clone)]
// pub struct Controller(Pool<AsyncPgConnection>);
// let config =
//    AsyncDieselConnectionManager::<diesel_async::AsyncPgConnection>::new(database_url);
// let pool = Pool::builder().max_size(30).build(config).await.unwrap();

impl Controller {
    pub async fn new(database_url: &str) -> Self {
        let main_connection = AsyncPgConnection::establish(database_url).await.unwrap();
        let did_cache_connection = AsyncPgConnection::establish(database_url).await.unwrap();

        info!("Created pool to database");
        Self {
            connection: Arc::new(Mutex::new(main_connection)),
            did_cache: DidCache::new(did_cache_connection).await,
        }
    }

    pub async fn push(&mut self, event: &ActionableEvent, cursor: u64) -> Result<()> {
        debug!("pushing event: {:?}", cursor);
        let mut conn = self.connection.lock().await;

        if let ActionableEvent::CreateLinks {
            record_id,
            links: _links,
        } = event
        {
            self.did_cache.get_did_id(&record_id.did.0).await?;
        }

        if cursor % 10000 == 0 {
            diesel::insert_into(bigint_keyvals::table)
                .values(BigintKeyval {
                    name: models::JETSTREAM_CURSOR_KEY.to_string(),
                    bivalue: cursor as i64,
                    updated_at: None,
                    created_at: None,
                })
                .on_conflict(bigint_keyvals::name)
                .do_update()
                .set(bigint_keyvals::bivalue.eq(cursor as i64))
                .execute(&mut conn)
                .await?;
        }
        Ok(())
    }

    pub async fn get_cursor(&self) -> Result<Option<u64>> {
        let mut conn = self.connection.lock().await;

        let result: Vec<i64> = bigint_keyvals::table
            .filter(bigint_keyvals::name.eq(models::JETSTREAM_CURSOR_KEY))
            .select(bigint_keyvals::bivalue)
            .load(&mut conn)
            .await?;

        if result.is_empty() {
            Ok(None)
        } else {
            Ok(Some(result[0] as u64))
        }

        //Ok(None)
    }
}

impl Drop for Controller {
    fn drop(&mut self) {
        error!("dropping the ball motherfucker")
    }
}

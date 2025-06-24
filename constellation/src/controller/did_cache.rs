use anyhow::anyhow;
use anyhow::Result;
use diesel::ExpressionMethods;
use diesel::QueryDsl;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

use crate::{models::Did, schema::dids};

pub struct DidCacheInternals {
    pub cache: HashMap<Vec<u8>, i64>,
    pub connection: AsyncPgConnection,
}

#[derive(Clone)]
pub struct DidCache(Arc<Mutex<DidCacheInternals>>);

impl DidCache {
    pub async fn new(connection: AsyncPgConnection) -> Self {
        Self(Arc::new(Mutex::new(DidCacheInternals {
            connection,
            cache: HashMap::new(),
        })))
    }

    pub async fn get_did_id(&mut self, did: &str) -> Result<i64> {
        let mut cache_internals = self.0.lock().await;
        let did_bytes = did.as_bytes().to_vec();
        let cached_value = cache_internals.cache.get(&did_bytes);
        if let Some(value) = cached_value {
            Ok(*value)
        } else {
            info!("cache size: {}", cache_internals.cache.len());
            diesel::insert_into(dids::table)
                .values(Did {
                    id: None,
                    did: did.to_string(),
                })
                .on_conflict(dids::did)
                .do_nothing()
                .returning(dids::id)
                .execute(&mut cache_internals.connection)
                .await?;

            let result: Vec<i64> = dids::table
                .filter(dids::did.eq(did))
                .select(dids::id)
                .load(&mut cache_internals.connection)
                .await?;

            cache_internals.cache.insert(did_bytes, result[0]);

            if result.is_empty() {
                Err(anyhow!("Something went wrong creating the did"))
            } else {
                Ok(result[0] as i64)
            }
        }
    }
}

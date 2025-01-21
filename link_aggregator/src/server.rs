use axum::{extract::Query, http, routing::get, Json, Router};
use serde::Deserialize;
use std::collections::HashMap;
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::task::block_in_place;
use tokio_util::sync::CancellationToken;

use crate::storage::LinkReader;

pub async fn serve<S, A>(store: S, addr: A, stay_alive: CancellationToken) -> anyhow::Result<()>
where
    S: LinkReader,
    A: ToSocketAddrs,
{
    let app = Router::new()
        .route("/", get(hello))
        .route(
            "/links/count",
            get({
                let store = store.clone();
                move |query| async { block_in_place(|| count_links(query, store)) }
            }),
        )
        .route(
            "/links/all/count",
            get({
                let store = store.clone();
                move |query| async { block_in_place(|| count_all_links(query, store)) }
            }),
        )
        .layer(axum_metrics::MetricLayer::default());

    let listener = TcpListener::bind(addr).await?;
    println!("api: listening at http://{:?}", listener.local_addr()?);
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { stay_alive.cancelled().await })
        .await?;

    Ok(())
}

async fn hello() -> &'static str {
    "helloooo\n"
}

#[derive(Deserialize)]
struct GetLinksQuery {
    target: String,
    collection: String,
    path: String,
}

fn count_links(
    query: Query<GetLinksQuery>,
    store: impl LinkReader,
) -> Result<String, http::StatusCode> {
    store
        .get_count(&query.target, &query.collection, &query.path)
        .map(|c| c.to_string())
        .map_err(|_| http::StatusCode::INTERNAL_SERVER_ERROR)
}

#[derive(Deserialize)]
struct GetAllLinksQuery {
    target: String,
}
type GetAllLinksResponse = HashMap<String, HashMap<String, u64>>;
fn count_all_links(
    query: Query<GetAllLinksQuery>,
    store: impl LinkReader,
) -> Result<Json<GetAllLinksResponse>, http::StatusCode> {
    store
        .get_all_counts(&query.target)
        .map(Json)
        .map_err(|_| http::StatusCode::INTERNAL_SERVER_ERROR)
}

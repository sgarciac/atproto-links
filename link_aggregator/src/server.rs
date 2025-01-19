use axum::{extract::Query, http, routing::get, Router};
use serde::Deserialize;
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::sync::oneshot::Receiver;
use tokio::task::block_in_place;

use crate::storage::LinkReader;

pub async fn serve<S, A>(store: S, addr: A, cancel: Receiver<()>) -> anyhow::Result<()>
where
    S: LinkReader,
    A: ToSocketAddrs,
{
    let app = Router::new()
        .route("/", get(hello))
        .route(
            "/links/count",
            get(move |query| async { block_in_place(|| count_links(query, store)) }),
        )
        .layer(axum_metrics::MetricLayer::default());

    let listener = TcpListener::bind(addr).await?;
    println!("api: listening at http://{:?}", listener.local_addr()?);
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            cancel.await.ok();
        })
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

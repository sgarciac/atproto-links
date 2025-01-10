use axum::{extract::Query, http, routing::get, Router};
use serde::Deserialize;
use std::marker::{Send, Sync};
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::task::block_in_place;

use crate::storage::LinkStorage;

pub async fn serve<S, A>(store: Arc<Mutex<S>>, addr: A) -> anyhow::Result<()>
where
    S: LinkStorage + Send + Sync + 'static,
    A: ToSocketAddrs,
{
    let app = Router::new().route("/", get(hello)).route(
        "/links/count",
        get(move |query| async { block_in_place(|| count_links(query, store)) }),
    );

    let listener = TcpListener::bind(addr).await?;
    println!("api: serving at http://{:?}", listener.local_addr()?);
    axum::serve(listener, app).await?;
    unreachable!()
}

async fn hello() -> &'static str {
    "helloooo\n"
}

#[derive(Deserialize)]
struct GetLinksQuery {
    target: String,
    path: String,
}

fn count_links(
    query: Query<GetLinksQuery>,
    store: Arc<Mutex<impl LinkStorage>>,
) -> Result<String, http::StatusCode> {
    { store.lock().unwrap().get_count(&query.target, &query.path) }
        .map(|c| c.to_string())
        .map_err(|_| http::StatusCode::INTERNAL_SERVER_ERROR)
}

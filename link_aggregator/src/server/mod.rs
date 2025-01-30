use askama::Template;
use axum::{
    extract::Query,
    http,
    response::{IntoResponse, Json},
    routing::get,
    Router,
};
use bincode::Options;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::collections::HashMap;
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::task::block_in_place;
use tokio_util::sync::CancellationToken;

use crate::storage::LinkReader;
use link_aggregator::RecordId;

mod acceptable;

use acceptable::{acceptable, ExtractAccept};

const DEFAULT_CURSOR_LIMIT: u64 = 16;
const DEFAULT_CURSOR_LIMIT_MAX: u64 = 100;

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
            "/links",
            get({
                let store = store.clone();
                move |query| async { block_in_place(|| get_links(query, store)) }
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

#[derive(Template, Serialize, Deserialize)]
#[template(path = "hello.html.j2")]
struct HelloReponse {}
async fn hello(accept: ExtractAccept) -> impl IntoResponse {
    let o = HelloReponse {};
    acceptable(accept, o)
}

#[derive(Deserialize)]
struct GetLinksCountQuery {
    target: String,
    collection: String,
    path: String,
}
fn count_links(
    query: Query<GetLinksCountQuery>,
    store: impl LinkReader,
) -> Result<String, http::StatusCode> {
    store
        .get_count(&query.target, &query.collection, &query.path)
        .map(|c| c.to_string())
        .map_err(|_| http::StatusCode::INTERNAL_SERVER_ERROR)
}

#[derive(Deserialize)]
struct GetLinkItemsQuery {
    target: String,
    collection: String,
    path: String,
    cursor: Option<OpaqueApiCursor>,
    limit: Option<u64>,
    // TODO: allow reverse (er, forward) order as well
}
#[derive(Serialize)]
struct GetLinkItemsResponse {
    // what does staleness mean?
    // - new links have appeared. would be nice to offer a `since` cursor to fetch these. and/or,
    // - links have been deleted. hmm.
    total: u64,
    linking_records: Vec<RecordId>,
    cursor: Option<OpaqueApiCursor>,
}
fn get_links(
    query: Query<GetLinkItemsQuery>,
    store: impl LinkReader,
) -> Result<Json<GetLinkItemsResponse>, http::StatusCode> {
    let until = query
        .cursor
        .clone()
        .map(|oc| ApiCursor::try_from(oc).map_err(|_| http::StatusCode::BAD_REQUEST))
        .transpose()?
        .map(|c| c.next);

    let limit = query.limit.unwrap_or(DEFAULT_CURSOR_LIMIT);
    if limit > DEFAULT_CURSOR_LIMIT_MAX {
        return Err(http::StatusCode::BAD_REQUEST);
    }

    let paged = store
        .get_links(&query.target, &query.collection, &query.path, limit, until)
        .map_err(|_| http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let cursor = paged.next.map(|next| {
        ApiCursor {
            version: paged.version,
            next,
        }
        .into()
    });

    Ok(Json(GetLinkItemsResponse {
        total: paged.version.0,
        linking_records: paged.items,
        cursor,
    }))
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

#[serde_as]
#[derive(Clone, Serialize, Deserialize)] // for json
struct OpaqueApiCursor(#[serde_as(as = "serde_with::hex::Hex")] Vec<u8>);

#[derive(Serialize, Deserialize)] // for bincode
struct ApiCursor {
    version: (u64, u64), // (collection length, deleted item count)
    next: u64,
}

impl TryFrom<OpaqueApiCursor> for ApiCursor {
    type Error = bincode::Error;

    fn try_from(item: OpaqueApiCursor) -> Result<Self, Self::Error> {
        bincode::DefaultOptions::new().deserialize(&item.0)
    }
}

impl From<ApiCursor> for OpaqueApiCursor {
    fn from(item: ApiCursor) -> Self {
        OpaqueApiCursor(bincode::DefaultOptions::new().serialize(&item).unwrap())
    }
}

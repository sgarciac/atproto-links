use crate::store::{Storage, StorageInfo};
use crate::{CreateRecord, Nsid};
use dropshot::endpoint;
use dropshot::ApiDescription;
use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingLevel;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::Query;
use dropshot::RequestContext;
use dropshot::ServerBuilder;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone)]
struct Context {
    pub spec: Arc<serde_json::Value>,
    storage: Storage,
}

/// Meta: get the openapi spec for this api
#[endpoint {
    method = GET,
    path = "/openapi",
}]
async fn get_openapi(
    ctx: RequestContext<Context>,
) -> Result<HttpResponseOk<serde_json::Value>, HttpError> {
    let spec = (*ctx.context().spec).clone();
    Ok(HttpResponseOk(spec))
}

#[derive(Debug, Serialize, JsonSchema)]
struct MetaInfo {
    storage_info: StorageInfo,
}
/// Get meta information about UFOs itself
#[endpoint {
    method = GET,
    path = "/meta"
}]
async fn get_meta_info(
    ctx: RequestContext<Context>,
) -> Result<HttpResponseOk<MetaInfo>, HttpError> {
    let Context { storage, .. } = ctx.context();
    let storage_info = storage
        .get_meta_info()
        .await
        .map_err(|e| HttpError::for_internal_error(format!("failed to get meta info: {e}")))?;
    Ok(HttpResponseOk(MetaInfo { storage_info }))
}

#[derive(Debug, Deserialize, JsonSchema)]
struct CollectionQuery {
    collection: String, // JsonSchema not implemented for Nsid :(
}
#[derive(Debug, Serialize, JsonSchema)]
struct ApiRecord {
    did: String,
    collection: String,
    rkey: String,
    record: serde_json::Value,
    time_us: u64,
}
impl ApiRecord {
    fn from_create_record(create_record: CreateRecord, collection: &Nsid) -> Self {
        let CreateRecord {
            did,
            rkey,
            record,
            cursor,
        } = create_record;
        Self {
            did: did.to_string(),
            collection: collection.to_string(),
            rkey: rkey.to_string(),
            record,
            time_us: cursor.to_raw_u64(),
        }
    }
}
/// Get recent records by collection
#[endpoint {
    method = GET,
    path = "/records",
}]
async fn get_records_by_collection(
    ctx: RequestContext<Context>,
    collection_query: Query<CollectionQuery>,
) -> Result<HttpResponseOk<Vec<ApiRecord>>, HttpError> {
    let Ok(collection) = Nsid::new(collection_query.into_inner().collection) else {
        return Err(HttpError::for_bad_request(
            None,
            "collection must be an NSID".to_string(),
        ));
    };
    let Context { storage, .. } = ctx.context();
    let records = storage
        .get_collection_records(&collection, 100)
        .await
        .map_err(|e| HttpError::for_internal_error(e.to_string()))?;

    if records.is_empty() {
        return Err(HttpError::for_not_found(
            None,
            format!("no saved records for collection {collection:?}"),
        ));
    }

    let api_records = records
        .into_iter()
        .map(|r| ApiRecord::from_create_record(r, &collection))
        .collect();

    Ok(HttpResponseOk(api_records))
}

pub async fn serve(storage: Storage) -> Result<(), String> {
    let log = ConfigLogging::StderrTerminal {
        level: ConfigLoggingLevel::Info,
    }
    .to_logger("hello-ufos")
    .map_err(|e| e.to_string())?;

    let mut api = ApiDescription::new();

    api.register(get_openapi).unwrap();
    api.register(get_meta_info).unwrap();
    api.register(get_records_by_collection).unwrap();

    let context = Context {
        spec: Arc::new(
            api.openapi("UFOs", semver::Version::new(0, 0, 0))
                .json()
                .map_err(|e| e.to_string())?,
        ),
        storage,
    };

    ServerBuilder::new(api, context, log)
        .config(ConfigDropshot {
            bind_address: "0.0.0.0:9999".parse().unwrap(),
            ..Default::default()
        })
        .start()
        .map_err(|error| format!("failed to start server: {}", error))?
        .await
}

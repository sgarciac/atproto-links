use crate::store::Storage;
use dropshot::endpoint;
use dropshot::ApiDescription;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingLevel;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::RequestContext;
use dropshot::ServerBuilder;

/// Get recent records by collection
#[endpoint {
    method = GET,
    path = "/records",
}]
async fn get_records_by_collection(
    _reqctx: RequestContext<Storage>,
) -> Result<HttpResponseOk<String>, HttpError> {
    Ok(HttpResponseOk("helloooooo".into()))
}

pub async fn serve(storage: Storage) -> Result<(), String> {
    let log = ConfigLogging::StderrTerminal {
        level: ConfigLoggingLevel::Info,
    }
    .to_logger("hello-ufos")
    .map_err(|e| e.to_string())?;

    let mut api = ApiDescription::new();
    api.register(get_records_by_collection).unwrap();

    ServerBuilder::new(api, storage, log)
        .start()
        .map_err(|error| format!("failed to start server: {}", error))?
        .await
}

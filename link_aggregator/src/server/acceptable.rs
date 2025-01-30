use askama::Template;
use axum::response::{Html, IntoResponse, Json, Response};
use axum_extra::TypedHeader;
use headers_accept::Accept;
use mediatype::names::{APPLICATION, HTML, JSON, TEXT};
use mediatype::MediaType;
use serde::Serialize;

const TEXT_HTML: MediaType = MediaType::new(TEXT, HTML);
const APPLICATION_JSON: MediaType = MediaType::new(APPLICATION, JSON);
const AVAILABLE: &[MediaType] = &[APPLICATION_JSON, TEXT_HTML];

pub type ExtractAccept = Option<TypedHeader<Accept>>;

pub fn acceptable<T: Serialize + Template>(accept: ExtractAccept, thing: T) -> Response {
    if let Some(accepting) = accept {
        if accepting.negotiate(AVAILABLE) == Some(&TEXT_HTML) {
            return Html(thing.render().unwrap()).into_response(); // TODO
        }
    }
    Json(thing).into_response()
}

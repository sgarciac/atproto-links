use askama::Template;
use axum::http::StatusCode;
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
            match thing.render() {
                Ok(content) => return Html(content).into_response(),
                Err(e) => {
                    eprintln!("template rendering failed: {e:?}");
                    return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                }
            }
        }
    }
    Json(thing).into_response()
}

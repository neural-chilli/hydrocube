// src/web/assets.rs
//
// Embedded static file serving via rust-embed.

use axum::extract::Path;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};

/// Embed everything under the `static/` directory at compile time.
#[derive(rust_embed::Embed)]
#[folder = "static/"]
struct StaticAssets;

/// Axum handler: serve a file from the embedded static assets.
///
/// Resolves the MIME type from the file extension, returns 404 if the file
/// does not exist.
pub async fn static_handler(Path(path): Path<String>) -> impl IntoResponse {
    let path = path.trim_start_matches('/');

    match StaticAssets::get(path) {
        Some(content) => {
            let mime = mime_guess::from_path(path)
                .first_or_octet_stream()
                .to_string();
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, mime)
                .body(axum::body::Body::from(content.data.to_vec()))
                .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
        }
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

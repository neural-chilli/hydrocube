// src/web/http_ingest.rs
//
// HTTP POST /ingest/{table} handler — accepts JSON objects or arrays,
// queues each row as a RawMessage to the engine ingest channel.

use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use std::sync::Arc;

use crate::config::DataFormat;
use crate::ingest::{IngestSender, RawMessage};
use crate::web::api::AppState;

/// POST /ingest/{table_name}
///
/// Accepts:
/// - `Content-Type: application/json` — single JSON object or JSON array
/// - `Content-Type: text/csv` — CSV bytes
///
/// Returns: `{"accepted": N}` on success.
/// Returns 404 if table not found, 400 on parse error, 503 if channel unavailable,
/// 429 if channel full.
pub async fn http_ingest_handler(
    State(state): State<Arc<AppState>>,
    Path(table_name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    if let Some(limiter) = state.rate_limiters.get(&table_name) {
        if limiter.check().is_err() {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                Json(serde_json::json!({ "error": "rate limit exceeded" })),
            )
                .into_response();
        }
    }

    // Validate table exists
    if state.config.table(&table_name).is_none() {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": format!("table '{}' not found", table_name) })),
        )
            .into_response();
    }

    // Determine format from Content-Type header
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json");

    let format = if content_type.contains("text/csv") {
        DataFormat::Csv
    } else {
        DataFormat::Json
    };

    // Parse body into individual row byte payloads
    let body_str = match std::str::from_utf8(&body) {
        Ok(s) => s,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "body must be valid UTF-8" })),
            )
                .into_response()
        }
    };

    let messages: Vec<Vec<u8>> = if format == DataFormat::Csv {
        // For CSV, forward the entire body as a single message
        vec![body.to_vec()]
    } else {
        match serde_json::from_str::<serde_json::Value>(body_str) {
            Ok(serde_json::Value::Array(arr)) => {
                arr.iter().map(|v| v.to_string().into_bytes()).collect()
            }
            Ok(obj) => vec![obj.to_string().into_bytes()],
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": format!("JSON parse error: {e}") })),
                )
                    .into_response()
            }
        }
    };

    let accepted = messages.len();

    // Get ingest channel
    let tx: &IngestSender = match &state.ingest_tx {
        Some(tx) => tx,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({ "error": "ingest channel not available" })),
            )
                .into_response()
        }
    };

    // Forward each row to the engine
    for bytes in messages {
        let raw = RawMessage {
            table: table_name.clone(),
            bytes,
            format: format.clone(),
        };
        if tx.try_send(raw).is_err() {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                Json(serde_json::json!({ "error": "ingest channel full" })),
            )
                .into_response();
        }
    }

    (
        StatusCode::OK,
        Json(serde_json::json!({ "accepted": accepted })),
    )
        .into_response()
}

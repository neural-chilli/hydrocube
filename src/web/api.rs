// src/web/api.rs
//
// AppState shared across all axum handlers, plus the REST API handler functions.

use std::sync::Arc;
use std::time::Instant;

use crate::aggregation::window::{compaction_cutoff, WINDOW_ID};
use crate::config::CubeConfig;
use crate::config::TableMode;
use crate::db_manager::{arrow::compute::concat_batches, DbManager};
use crate::error::HcError;
use crate::publish::{batch_to_base64_arrow, DeltaEvent};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::broadcast;

use std::sync::atomic::Ordering;

// ---------------------------------------------------------------------------
// AppState
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct AppState {
    pub db: DbManager,
    pub config: CubeConfig,
    /// Full aggregation SQL with synthetic `_key` column injected.
    /// Used by the snapshot endpoint so Perspective's `{ index: '_key' }` is valid.
    pub snapshot_sql: String,
    pub start_time: Instant,
    pub broadcast_tx: broadcast::Sender<DeltaEvent>,
    /// Optional channel for HTTP-based ingest. `None` when the engine is not
    /// running (e.g. validate-only mode) or the channel is not wired up.
    pub ingest_tx: Option<crate::ingest::IngestSender>,
}

// ---------------------------------------------------------------------------
// Helper: convert HcError to an HTTP 500 response
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct ApiError(HcError);

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let body = json!({ "error": self.0.to_string() });
        (StatusCode::INTERNAL_SERVER_ERROR, Json(body)).into_response()
    }
}

impl<E: Into<HcError>> From<E> for ApiError {
    fn from(e: E) -> Self {
        ApiError(e.into())
    }
}

type ApiResult<T> = Result<T, ApiError>;

// ---------------------------------------------------------------------------
// Helper: concatenate Arrow batches and serialize to base64
// ---------------------------------------------------------------------------

fn concat_and_serialize(
    batches: &[crate::db_manager::RecordBatch],
) -> Result<(usize, String), ApiError> {
    if batches.is_empty() {
        return Ok((0, String::new()));
    }
    let schema = batches[0].schema();
    let merged = concat_batches(&schema, batches)
        .map_err(|e| ApiError(HcError::Publish(format!("Arrow concat error: {e}"))))?;
    let row_count = merged.num_rows();
    let b64 = batch_to_base64_arrow(&merged).map_err(ApiError)?;
    Ok((row_count, b64))
}

// ---------------------------------------------------------------------------
// GET /api/status
// ---------------------------------------------------------------------------

#[derive(Serialize)]
pub struct StatusResponse {
    pub cube_name: String,
    pub status: &'static str,
    pub uptime_secs: u64,
    pub window_id: u64,
    pub compaction_cutoff: u64,
    pub sse_client_count: usize,
    pub table_count: usize,
    pub source_count: usize,
}

pub async fn status_handler(State(state): State<Arc<AppState>>) -> ApiResult<Json<StatusResponse>> {
    let uptime = state.start_time.elapsed().as_secs();
    let window_id = WINDOW_ID.load(Ordering::Relaxed);
    let cutoff = compaction_cutoff();
    let sse_clients = state.broadcast_tx.receiver_count();

    Ok(Json(StatusResponse {
        cube_name: state.config.name.clone(),
        status: "running",
        uptime_secs: uptime,
        window_id,
        compaction_cutoff: cutoff,
        sse_client_count: sse_clients,
        table_count: state.config.tables.len(),
        source_count: state.config.sources.len(),
    }))
}

// ---------------------------------------------------------------------------
// GET /api/schema
// ---------------------------------------------------------------------------

#[derive(Serialize)]
pub struct SchemaResponse {
    pub cube_name: String,
    pub columns: Vec<ColumnInfo>,
    pub aggregation_sql: String,
}

#[derive(Serialize)]
pub struct ColumnInfo {
    pub name: String,
    #[serde(rename = "type")]
    pub col_type: String,
}

pub async fn schema_handler(State(state): State<Arc<AppState>>) -> ApiResult<Json<SchemaResponse>> {
    let columns = state
        .config
        .tables
        .first()
        .map(|t| t.schema.columns.as_slice())
        .unwrap_or(&[])
        .iter()
        .map(|c| ColumnInfo {
            name: c.name.clone(),
            col_type: c.col_type.clone(),
        })
        .collect();

    Ok(Json(SchemaResponse {
        cube_name: state.config.name.clone(),
        columns,
        aggregation_sql: state.config.aggregation.publish.sql.clone(),
    }))
}

// ---------------------------------------------------------------------------
// GET /api/snapshot
// ---------------------------------------------------------------------------

#[derive(Serialize)]
pub struct SnapshotResponse {
    pub row_count: usize,
    pub arrow_ipc_b64: String,
}

pub async fn snapshot_handler(
    State(state): State<Arc<AppState>>,
) -> ApiResult<Json<SnapshotResponse>> {
    let batches = state
        .db
        .query_arrow(&state.snapshot_sql)
        .await
        .map_err(ApiError::from)?;

    let (row_count, arrow_ipc_b64) = concat_and_serialize(&batches)?;

    Ok(Json(SnapshotResponse {
        row_count,
        arrow_ipc_b64,
    }))
}

// ---------------------------------------------------------------------------
// POST /api/query
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct QueryRequest {
    pub sql: String,
}

#[derive(Serialize)]
pub struct QueryResponse {
    pub row_count: usize,
    pub arrow_ipc_b64: String,
}

pub async fn query_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<QueryRequest>,
) -> ApiResult<Json<QueryResponse>> {
    let batches = state
        .db
        .query_arrow(&body.sql)
        .await
        .map_err(ApiError::from)?;

    let (row_count, arrow_ipc_b64) = concat_and_serialize(&batches)?;

    Ok(Json(QueryResponse {
        row_count,
        arrow_ipc_b64,
    }))
}

// ---------------------------------------------------------------------------
// GET /api/drillthrough/{table}
// ---------------------------------------------------------------------------

pub async fn drillthrough_handler(
    State(state): State<Arc<AppState>>,
    Path(table_name): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    // Validate table exists and is append-mode
    let table_cfg = match state.config.table(&table_name) {
        Some(t) => t,
        None => {
            let body = json!({ "error": format!("table '{}' not found", table_name) });
            return Err((StatusCode::BAD_REQUEST, Json(body)));
        }
    };

    if table_cfg.mode != TableMode::Append {
        let body =
            json!({ "error": format!("table '{}' is not an append-mode table", table_name) });
        return Err((StatusCode::BAD_REQUEST, Json(body)));
    }

    let max_rows = state.config.drillthrough.max_rows;

    // Count rows first to enforce limit
    let count_sql = format!("SELECT COUNT(*) AS cnt FROM {}", table_name);
    let count_rows = state.db.query_json(&count_sql, vec![]).await.map_err(|e| {
        let body = json!({ "error": e.to_string() });
        (StatusCode::INTERNAL_SERVER_ERROR, Json(body))
    })?;

    let row_count = count_rows
        .first()
        .and_then(|r| r.get("cnt"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as usize;

    if row_count > max_rows {
        let body = json!({
            "error": format!("result set too large: {} rows exceeds max_rows={}", row_count, max_rows)
        });
        return Err((StatusCode::PAYLOAD_TOO_LARGE, Json(body)));
    }

    // Fetch rows as Arrow IPC
    let fetch_sql = format!("SELECT * FROM {}", table_name);
    let batches = state.db.query_arrow(fetch_sql).await.map_err(|e| {
        let body = json!({ "error": e.to_string() });
        (StatusCode::INTERNAL_SERVER_ERROR, Json(body))
    })?;

    let (actual_rows, b64) = concat_and_serialize(&batches).map_err(|e| {
        let body = json!({ "error": e.0.to_string() });
        (StatusCode::INTERNAL_SERVER_ERROR, Json(body))
    })?;

    Ok(Json(json!({
        "table": table_name,
        "row_count": actual_rows,
        "data": b64,
    })))
}

// ---------------------------------------------------------------------------
// POST /api/reaggregate
// ---------------------------------------------------------------------------

pub async fn reaggregate_handler(
    State(state): State<Arc<AppState>>,
) -> ApiResult<Json<serde_json::Value>> {
    // Re-run the startup hook SQL if declared
    if let Some(startup) = &state.config.aggregation.startup {
        if let Some(sql) = &startup.sql {
            use crate::aggregation::window::compaction_cutoff;
            use crate::hooks::placeholder::PlaceholderContext;
            let mut ctx =
                PlaceholderContext::new(compaction_cutoff(), None, chrono::Utc::now(), None);
            ctx.table_modes = state
                .config
                .tables
                .iter()
                .map(|t| (t.name.clone(), t.mode.clone()))
                .collect();
            let expanded = ctx.expand(sql);
            // Execute each semicolon-separated statement
            for stmt in expanded.split(';') {
                let trimmed = stmt.trim();
                if !trimmed.is_empty() {
                    state.db.execute(trimmed, vec![]).await?;
                }
            }
        }
    }

    Ok(Json(
        json!({ "status": "ok", "message": "re-aggregation complete" }),
    ))
}

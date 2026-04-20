// src/web/api.rs
//
// AppState shared across all axum handlers, plus the REST API handler functions.

use std::sync::Arc;
use std::time::Instant;

use crate::aggregation::window::{compaction_cutoff, WINDOW_ID};
use crate::config::CubeConfig;
use crate::db_manager::{arrow::compute::concat_batches, DbManager};
use crate::error::HcError;
use crate::publish::{batch_to_base64_arrow, DeltaEvent};
use axum::extract::State;
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
}

// ---------------------------------------------------------------------------
// Helper: convert HcError to an HTTP 500 response
// ---------------------------------------------------------------------------

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
        .schema
        .columns
        .iter()
        .map(|c| ColumnInfo {
            name: c.name.clone(),
            col_type: c.col_type.clone(),
        })
        .collect();

    Ok(Json(SchemaResponse {
        cube_name: state.config.name.clone(),
        columns,
        aggregation_sql: state.config.aggregation.sql.clone(),
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

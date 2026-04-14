// src/web/api.rs
//
// AppState shared across all axum handlers, plus the REST API handler functions.

use std::sync::Arc;
use std::time::Instant;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::broadcast;
use tracing::error;

use crate::aggregation::window::{compaction_cutoff, WINDOW_ID};
use crate::config::CubeConfig;
use crate::db_manager::DbManager;
use crate::error::HcError;
use crate::publish::{batch_to_base64_arrow, DeltaEvent};

use std::sync::atomic::Ordering;

// ---------------------------------------------------------------------------
// AppState
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct AppState {
    pub db: DbManager,
    pub config: CubeConfig,
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
    let sql = &state.config.aggregation.sql;
    let batches = state.db.query_arrow(sql).await.map_err(ApiError::from)?;

    let mut row_count = 0usize;
    let mut all_b64 = String::new();

    for batch in &batches {
        row_count += batch.num_rows();
        match batch_to_base64_arrow(batch) {
            Ok(b64) => all_b64 = b64,
            Err(e) => {
                error!(target: "hydrocube::web::api", "snapshot serialization error: {e}");
                return Err(ApiError(e));
            }
        }
    }

    if batches.is_empty() {
        // Return an empty response with row_count 0 and empty base64
        return Ok(Json(SnapshotResponse {
            row_count: 0,
            arrow_ipc_b64: String::new(),
        }));
    }

    Ok(Json(SnapshotResponse {
        row_count,
        arrow_ipc_b64: all_b64,
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

    let mut row_count = 0usize;
    let mut last_b64 = String::new();

    for batch in &batches {
        row_count += batch.num_rows();
        match batch_to_base64_arrow(batch) {
            Ok(b64) => last_b64 = b64,
            Err(e) => {
                error!(target: "hydrocube::web::api", "query serialization error: {e}");
                return Err(ApiError(e));
            }
        }
    }

    Ok(Json(QueryResponse {
        row_count,
        arrow_ipc_b64: last_b64,
    }))
}

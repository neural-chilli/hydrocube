// src/web/api.rs
//
// AppState shared across all axum handlers, plus the REST API handler functions.

use std::sync::Arc;
use std::time::Instant;

use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use std::num::NonZeroU32;

pub type RateLimiters = std::collections::HashMap<String, DefaultDirectRateLimiter>;

pub fn build_rate_limiters(config: &crate::config::CubeConfig) -> RateLimiters {
    let mut map = RateLimiters::new();
    for src in &config.sources {
        if src.source_type == crate::config::SourceType::Http {
            if let Some(rpm) = src.rate_limit_per_minute {
                if let Some(nz) = NonZeroU32::new(rpm) {
                    let quota = Quota::per_minute(nz);
                    map.insert(src.table.clone(), RateLimiter::direct(quota));
                }
            }
        }
    }
    map
}

use crate::aggregation::window::{compaction_cutoff, WINDOW_ID};
use crate::config::CubeConfig;
use crate::config::TableMode;
use crate::db_manager::{arrow::compute::concat_batches, DbManager};
use crate::error::HcError;
use crate::peers::{PeerRecord, PeerRegistry, PeerStatus};
use crate::publish::{batch_to_base64_arrow, DeltaEvent};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::broadcast;

use std::sync::atomic::{AtomicU64, Ordering};

// ---------------------------------------------------------------------------
// ErrorCounters
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
pub struct ErrorCounters {
    pub parse_errors: std::sync::Arc<DashMap<String, AtomicU64>>,
    pub schema_errors: std::sync::Arc<DashMap<String, AtomicU64>>,
}

impl ErrorCounters {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn inc_parse(&self, table: &str) {
        self.parse_errors
            .entry(table.to_owned())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_schema(&self, table: &str) {
        self.schema_errors
            .entry(table.to_owned())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }
}

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
    /// Optional peer registry for multi-cube peer discovery.
    pub peer_registry: Option<Arc<PeerRegistry>>,
    /// HTTP client for forwarding peer registrations.
    pub http_client: reqwest::Client,
    /// Per-table parse and schema error counters.
    pub error_counters: ErrorCounters,
    /// Per-table rate limiters for HTTP ingest.
    pub rate_limiters: Arc<RateLimiters>,
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
    pub parse_errors: std::collections::HashMap<String, u64>,
    pub schema_errors: std::collections::HashMap<String, u64>,
}

pub async fn status_handler(State(state): State<Arc<AppState>>) -> ApiResult<Json<StatusResponse>> {
    let uptime = state.start_time.elapsed().as_secs();
    let window_id = WINDOW_ID.load(Ordering::Relaxed);
    let cutoff = compaction_cutoff();
    let sse_clients = state.broadcast_tx.receiver_count();

    let parse_errors: std::collections::HashMap<String, u64> = state
        .error_counters
        .parse_errors
        .iter()
        .map(|e| (e.key().clone(), e.value().load(Ordering::Relaxed)))
        .collect();

    let schema_errors: std::collections::HashMap<String, u64> = state
        .error_counters
        .schema_errors
        .iter()
        .map(|e| (e.key().clone(), e.value().load(Ordering::Relaxed)))
        .collect();

    Ok(Json(StatusResponse {
        cube_name: state.config.name.clone(),
        status: "running",
        uptime_secs: uptime,
        window_id,
        compaction_cutoff: cutoff,
        sse_client_count: sse_clients,
        table_count: state.config.tables.len(),
        source_count: state.config.sources.len(),
        parse_errors,
        schema_errors,
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

// ---------------------------------------------------------------------------
// GET /api/peers
// ---------------------------------------------------------------------------

#[derive(Serialize)]
pub struct GetPeersResponse {
    #[serde(rename = "self")]
    pub self_info: PeerRecord,
    pub peers: Vec<PeerRecord>,
}

pub async fn get_peers_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match &state.peer_registry {
        None => {
            let self_info = PeerRecord {
                name: state.config.name.clone(),
                url: String::new(),
                description: state.config.description.clone().unwrap_or_default(),
                status: PeerStatus::Online,
            };
            Json(GetPeersResponse {
                self_info,
                peers: vec![],
            })
            .into_response()
        }
        Some(registry) => {
            let self_info = registry.own_info();
            let peers = registry.list();
            Json(GetPeersResponse { self_info, peers }).into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// POST /api/peers/register
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct RegisterPeerRequest {
    pub name: String,
    pub url: String,
    pub description: String,
    #[serde(default)]
    pub forwarded: bool,
}

#[derive(Serialize)]
struct ForwardBody<'a> {
    name: &'a str,
    url: &'a str,
    description: &'a str,
    forwarded: bool,
}

pub async fn register_peer_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<RegisterPeerRequest>,
) -> impl IntoResponse {
    let registry = match &state.peer_registry {
        Some(r) => r.clone(),
        None => return Json(Vec::<PeerRecord>::new()).into_response(),
    };

    registry.upsert(PeerRecord {
        name: body.name.clone(),
        url: body.url.clone(),
        description: body.description.clone(),
        status: PeerStatus::Online,
    });

    if !body.forwarded {
        let forward_targets = registry.peer_urls_except(&body.url);
        if !forward_targets.is_empty() {
            let client = state.http_client.clone();
            let name = body.name.clone();
            let url = body.url.clone();
            let description = body.description.clone();
            tokio::spawn(async move {
                for target_base in forward_targets {
                    let forward_url =
                        format!("{}/api/peers/register", target_base.trim_end_matches('/'));
                    let fw = ForwardBody {
                        name: &name,
                        url: &url,
                        description: &description,
                        forwarded: true,
                    };
                    if let Err(e) = client.post(&forward_url).json(&fw).send().await {
                        tracing::warn!(target: "hydrocube::peers", "Failed to forward registration to {}: {}", target_base, e);
                    }
                }
            });
        }
    }

    Json(registry.list()).into_response()
}

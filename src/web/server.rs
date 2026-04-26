// src/web/server.rs
//
// Build and start the axum HTTP server.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use axum::Router;
use minijinja::Environment;
use tokio::sync::broadcast;
use tracing::info;

use crate::aggregation::sql_gen::AggSqlGenerator;
use crate::config::CubeConfig;
use crate::db_manager::DbManager;
use crate::error::{HcError, HcResult};
use crate::publish::DeltaEvent;

use super::api::{drillthrough_handler, query_handler, reaggregate_handler, schema_handler, snapshot_handler, status_handler, AppState};
use super::assets::static_handler;

// ---------------------------------------------------------------------------
// SSE handler — extracts broadcast_tx from AppState and delegates to sse module
// ---------------------------------------------------------------------------

async fn sse_handler(
    State(state): State<Arc<AppState>>,
) -> axum::response::sse::Sse<
    impl futures::Stream<Item = Result<axum::response::sse::Event, std::convert::Infallible>>,
> {
    let rx = state.broadcast_tx.subscribe();
    axum::response::sse::Sse::new(super::sse::sse_stream(rx))
}

// ---------------------------------------------------------------------------
// Index route — render the Minijinja template
// ---------------------------------------------------------------------------

async fn index_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let template_src = include_str!("../../templates/index.html.j2");

    let mut env = Environment::new();
    if let Err(e) = env.add_template("index", template_src) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Html(format!("Template load error: {e}")),
        )
            .into_response();
    }

    let tmpl = match env.get_template("index") {
        Ok(t) => t,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Html(format!("Template get error: {e}")),
            )
                .into_response();
        }
    };

    let ctx = minijinja::context! {
        cube_name => state.config.name.clone(),
    };

    match tmpl.render(ctx) {
        Ok(html) => Html(html).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Html(format!("Template render error: {e}")),
        )
            .into_response(),
    }
}

// ---------------------------------------------------------------------------
// start_server
// ---------------------------------------------------------------------------

/// Build the axum router and start listening.
///
/// This function runs until the server shuts down (i.e. it does not return
/// on success — call it as a spawned task or the last thing in main).
pub async fn start_server(
    db: DbManager,
    config: CubeConfig,
    broadcast_tx: broadcast::Sender<DeltaEvent>,
    port: u16,
) -> HcResult<()> {
    // Pre-compute the snapshot SQL with synthetic _key column so the snapshot
    // endpoint returns the same schema as the engine's delta broadcasts.
    let snapshot_sql = AggSqlGenerator::from_user_sql(&config.aggregation.publish.sql)
        .map(|g| g.full_aggregation_sql_with_key())
        .unwrap_or_else(|_| config.aggregation.publish.sql.clone());

    let state = Arc::new(AppState {
        db,
        config,
        snapshot_sql,
        start_time: std::time::Instant::now(),
        broadcast_tx,
    });

    let router = Router::new()
        // REST API
        .route("/api/status", get(status_handler))
        .route("/api/schema", get(schema_handler))
        .route("/api/snapshot", get(snapshot_handler))
        .route("/api/query", post(query_handler))
        .route("/api/drillthrough/{table}", get(drillthrough_handler))
        .route("/api/reaggregate", post(reaggregate_handler))
        // SSE stream
        .route("/api/stream", get(sse_handler))
        // Static assets
        .route("/assets/{*path}", get(static_handler))
        // Index page
        .route("/", get(index_handler))
        .with_state(state);

    let addr: SocketAddr = format!("0.0.0.0:{port}")
        .parse()
        .map_err(|e| HcError::Web(format!("invalid bind address 0.0.0.0:{port}: {e}")))?;

    info!(
        target: "hydrocube::web",
        "HydroCube UI listening on http://{}",
        addr
    );

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| HcError::Web(format!("bind failed on {addr}: {e}")))?;

    axum::serve(listener, router)
        .await
        .map_err(|e| HcError::Web(format!("server error: {e}")))?;

    Ok(())
}

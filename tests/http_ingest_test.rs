// tests/http_ingest_test.rs
//
// Integration tests for POST /ingest/{table}

use std::sync::Arc;
use std::time::Instant;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::post;
use axum::Router;
use hydrocube::config::CubeConfig;
use hydrocube::db_manager::DbManager;
use hydrocube::publish::DeltaEvent;
use hydrocube::web::api::AppState;
use hydrocube::web::http_ingest::http_ingest_handler;
use tokio::sync::{broadcast, mpsc};
use tower::ServiceExt;

fn test_config() -> CubeConfig {
    serde_yaml::from_str(r#"
name: test
tables:
  - name: trades
    mode: append
    schema:
      columns:
        - { name: book, type: VARCHAR }
        - { name: notional, type: DOUBLE }
sources: []
window: { interval_ms: 100 }
persistence: { enabled: false, path: ":memory:", flush_interval: 1 }
aggregation:
  key_columns: [book]
  publish:
    sql: "SELECT book, SUM(notional) AS total FROM trades GROUP BY book"
"#).unwrap()
}

fn make_state(ingest_tx: Option<hydrocube::ingest::IngestSender>) -> Arc<AppState> {
    let db = DbManager::open_in_memory().unwrap();
    let (broadcast_tx, _) = broadcast::channel::<DeltaEvent>(16);
    Arc::new(AppState {
        db,
        config: test_config(),
        snapshot_sql: "SELECT 1".to_owned(),
        start_time: Instant::now(),
        broadcast_tx,
        ingest_tx,
    })
}

fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/ingest/{table}", post(http_ingest_handler))
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Test 1: unknown table → 404
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_http_ingest_unknown_table_returns_404() {
    let state = make_state(None);
    let router = build_router(state);

    let request = Request::builder()
        .method("POST")
        .uri("/ingest/nonexistent_table")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"book":"A","notional":100.0}"#))
        .unwrap();

    let response = router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert!(json["error"].as_str().unwrap().contains("nonexistent_table"));
}

// ---------------------------------------------------------------------------
// Test 2: known table but no channel → 503
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_http_ingest_no_channel_returns_503() {
    let state = make_state(None);
    let router = build_router(state);

    let request = Request::builder()
        .method("POST")
        .uri("/ingest/trades")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"book":"A","notional":100.0}"#))
        .unwrap();

    let response = router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert!(json["error"].as_str().unwrap().contains("not available"));
}

// ---------------------------------------------------------------------------
// Test 3: valid JSON object with live channel → 200 + accepted=1
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_http_ingest_single_object_accepted() {
    let (tx, mut rx) = mpsc::channel::<hydrocube::ingest::RawMessage>(16);
    let state = make_state(Some(tx));
    let router = build_router(state);

    let request = Request::builder()
        .method("POST")
        .uri("/ingest/trades")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"book":"A","notional":100.0}"#))
        .unwrap();

    let response = router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(json["accepted"].as_u64().unwrap(), 1);

    // Verify the message arrived on the channel
    let msg = rx.try_recv().expect("expected a message on the channel");
    assert_eq!(msg.table, "trades");
}

// ---------------------------------------------------------------------------
// Test 4: JSON array → accepted=N
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_http_ingest_json_array_accepted() {
    let (tx, mut rx) = mpsc::channel::<hydrocube::ingest::RawMessage>(16);
    let state = make_state(Some(tx));
    let router = build_router(state);

    let request = Request::builder()
        .method("POST")
        .uri("/ingest/trades")
        .header("content-type", "application/json")
        .body(Body::from(r#"[{"book":"A","notional":100.0},{"book":"B","notional":200.0}]"#))
        .unwrap();

    let response = router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(json["accepted"].as_u64().unwrap(), 2);

    let msg1 = rx.try_recv().unwrap();
    assert_eq!(msg1.table, "trades");
    let msg2 = rx.try_recv().unwrap();
    assert_eq!(msg2.table, "trades");
}

// ---------------------------------------------------------------------------
// Test 5: invalid JSON → 400
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_http_ingest_invalid_json_returns_400() {
    let (tx, _rx) = mpsc::channel::<hydrocube::ingest::RawMessage>(16);
    let state = make_state(Some(tx));
    let router = build_router(state);

    let request = Request::builder()
        .method("POST")
        .uri("/ingest/trades")
        .header("content-type", "application/json")
        .body(Body::from(b"not json at all".as_ref()))
        .unwrap();

    let response = router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

use axum::extract::{Path, State};
use hydrocube::db_manager::DbManager;
use hydrocube::publish::DeltaEvent;
use hydrocube::web::api::{drillthrough_handler, reaggregate_handler, status_handler, AppState};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::broadcast;

fn make_state(config: hydrocube::config::CubeConfig, db: DbManager) -> Arc<AppState> {
    let (broadcast_tx, _) = broadcast::channel::<DeltaEvent>(16);
    Arc::new(AppState {
        db,
        snapshot_sql: "SELECT 1".to_owned(),
        config,
        start_time: Instant::now(),
        broadcast_tx,
        ingest_tx: None,
        peer_registry: None,
        http_client: reqwest::Client::new(),
    })
}

fn trades_config() -> hydrocube::config::CubeConfig {
    serde_yaml::from_str(
        r#"
name: t
tables:
  - name: trades
    mode: append
    schema:
      columns:
        - { name: id,  type: VARCHAR }
        - { name: val, type: DOUBLE }
  - name: md
    mode: replace
    key_columns: [id]
    schema:
      columns:
        - { name: id,  type: VARCHAR }
        - { name: px,  type: DOUBLE }
sources: []
window: { interval_ms: 1000 }
persistence: { enabled: false, path: ":memory:", flush_interval: 10 }
aggregation:
  key_columns: [id]
  publish:
    sql: "SELECT id, SUM(val) FROM {trades} GROUP BY id"
"#,
    )
    .unwrap()
}

#[tokio::test]
async fn test_drillthrough_unknown_table_returns_400() {
    let db = DbManager::open_in_memory().unwrap();
    let state = make_state(trades_config(), db);
    let result = drillthrough_handler(State(state), Path("nonexistent".to_owned())).await;
    assert!(result.is_err());
    let (status, _) = result.unwrap_err();
    use axum::http::StatusCode;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_drillthrough_replace_table_returns_400() {
    let db = DbManager::open_in_memory().unwrap();
    db.execute(
        "CREATE TABLE md (id VARCHAR, px DOUBLE, PRIMARY KEY (id))",
        vec![],
    )
    .await
    .unwrap();
    let state = make_state(trades_config(), db);
    let result = drillthrough_handler(State(state), Path("md".to_owned())).await;
    assert!(result.is_err());
    let (status, _) = result.unwrap_err();
    use axum::http::StatusCode;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_drillthrough_returns_rows() {
    let db = DbManager::open_in_memory().unwrap();
    db.execute(
        "CREATE TABLE trades (id VARCHAR, val DOUBLE, _window_id UBIGINT)",
        vec![],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO trades VALUES ('A', 1.0, 1), ('B', 2.0, 1)",
        vec![],
    )
    .await
    .unwrap();

    let state = make_state(trades_config(), db);
    let result = drillthrough_handler(State(state), Path("trades".to_owned())).await;
    assert!(result.is_ok());
    let axum::response::Json(body) = result.unwrap();
    assert_eq!(body["row_count"].as_u64().unwrap(), 2);
}

#[tokio::test]
async fn test_reaggregate_no_startup_hook_is_noop() {
    let db = DbManager::open_in_memory().unwrap();
    let cfg = trades_config(); // no startup hook
    let state = make_state(cfg, db);
    let result = reaggregate_handler(State(state)).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_status_includes_table_and_source_counts() {
    let db = DbManager::open_in_memory().unwrap();
    let cfg = trades_config(); // has 2 tables, 0 sources
    let state = make_state(cfg, db);
    let result = status_handler(State(state)).await;
    assert!(result.is_ok());
    let axum::response::Json(resp) = result.unwrap();
    assert_eq!(resp.table_count, 2);
    assert_eq!(resp.source_count, 0);
}

// ---------------------------------------------------------------------------
// Peer API tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_get_peers_no_registry_returns_self_with_empty_peers() {
    use hydrocube::web::api::get_peers_handler;
    let db = DbManager::open_in_memory().unwrap();
    let cfg = trades_config();
    let state = make_state(cfg, db);
    // peer_registry is None — handler should return self_info from config and empty peers
    let result = get_peers_handler(State(state.clone())).await;
    let response = axum::response::IntoResponse::into_response(result);
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(body["peers"].as_array().unwrap().len(), 0);
    assert_eq!(body["self"]["name"].as_str().unwrap(), &state.config.name);
}

#[tokio::test]
async fn test_register_peer_no_registry_returns_empty_list() {
    use hydrocube::web::api::register_peer_handler;
    use axum::Json;
    let db = DbManager::open_in_memory().unwrap();
    let state = make_state(trades_config(), db);
    let body = hydrocube::web::api::RegisterPeerRequest {
        name: "peer1".to_owned(),
        url: "http://peer1:8080".to_owned(),
        description: "a peer".to_owned(),
        forwarded: false,
    };
    let result = register_peer_handler(State(state), Json(body)).await;
    let response = axum::response::IntoResponse::into_response(result);
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let peers: Vec<serde_json::Value> = serde_json::from_slice(&body_bytes).unwrap();
    assert!(peers.is_empty());
}

#[tokio::test]
async fn test_get_peers_with_registry_returns_peer_list() {
    use hydrocube::peers::{PeerRecord, PeerRegistry, PeerStatus};
    use hydrocube::web::api::get_peers_handler;
    let db = DbManager::open_in_memory().unwrap();
    let cfg = trades_config();
    let (broadcast_tx, _) = broadcast::channel::<DeltaEvent>(16);
    let own = PeerRecord {
        name: cfg.name.clone(),
        url: "http://self:8080".to_owned(),
        description: String::new(),
        status: PeerStatus::Online,
    };
    let registry = PeerRegistry::new(own);
    registry.upsert(PeerRecord {
        name: "peer-a".to_owned(),
        url: "http://peer-a:8080".to_owned(),
        description: "Peer A".to_owned(),
        status: PeerStatus::Online,
    });
    let state = Arc::new(AppState {
        db,
        snapshot_sql: "SELECT 1".to_owned(),
        config: cfg,
        start_time: Instant::now(),
        broadcast_tx,
        ingest_tx: None,
        peer_registry: Some(Arc::new(registry)),
        http_client: reqwest::Client::new(),
    });
    let result = get_peers_handler(State(state)).await;
    let response = axum::response::IntoResponse::into_response(result);
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(body["peers"].as_array().unwrap().len(), 1);
    assert_eq!(body["peers"][0]["name"].as_str().unwrap(), "peer-a");
}

#[tokio::test]
async fn test_register_peer_adds_to_registry() {
    use hydrocube::peers::{PeerRecord, PeerRegistry, PeerStatus};
    use hydrocube::web::api::register_peer_handler;
    use axum::Json;
    let db = DbManager::open_in_memory().unwrap();
    let cfg = trades_config();
    let (broadcast_tx, _) = broadcast::channel::<DeltaEvent>(16);
    let own = PeerRecord {
        name: cfg.name.clone(),
        url: "http://self:8080".to_owned(),
        description: String::new(),
        status: PeerStatus::Online,
    };
    let registry = Arc::new(PeerRegistry::new(own));
    let state = Arc::new(AppState {
        db,
        snapshot_sql: "SELECT 1".to_owned(),
        config: cfg,
        start_time: Instant::now(),
        broadcast_tx,
        ingest_tx: None,
        peer_registry: Some(registry.clone()),
        http_client: reqwest::Client::new(),
    });
    let body = hydrocube::web::api::RegisterPeerRequest {
        name: "peer-b".to_owned(),
        url: "http://peer-b:9090".to_owned(),
        description: "Peer B".to_owned(),
        forwarded: true,
    };
    let result = register_peer_handler(State(state), Json(body)).await;
    let response = axum::response::IntoResponse::into_response(result);
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let listed = registry.list();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].name, "peer-b");
}

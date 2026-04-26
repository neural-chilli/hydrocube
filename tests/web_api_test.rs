use std::sync::Arc;
use std::time::Instant;
use axum::extract::{Path, State};
use hydrocube::db_manager::DbManager;
use hydrocube::web::api::{AppState, drillthrough_handler, reaggregate_handler, status_handler};
use hydrocube::publish::DeltaEvent;
use tokio::sync::broadcast;

fn make_state(config: hydrocube::config::CubeConfig, db: DbManager) -> Arc<AppState> {
    let (broadcast_tx, _) = broadcast::channel::<DeltaEvent>(16);
    Arc::new(AppState {
        db,
        snapshot_sql: "SELECT 1".to_owned(),
        config,
        start_time: Instant::now(),
        broadcast_tx,
    })
}

fn trades_config() -> hydrocube::config::CubeConfig {
    serde_yaml::from_str(r#"
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
"#).unwrap()
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
    db.execute("CREATE TABLE md (id VARCHAR, px DOUBLE, PRIMARY KEY (id))", vec![]).await.unwrap();
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
    ).await.unwrap();
    db.execute("INSERT INTO trades VALUES ('A', 1.0, 1), ('B', 2.0, 1)", vec![]).await.unwrap();

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

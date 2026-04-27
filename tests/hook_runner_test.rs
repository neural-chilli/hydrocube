use hydrocube::db_manager::DbManager;
use hydrocube::hooks::runner::HookRunner;

#[tokio::test]
async fn test_hook_runner_resolves_static_file_paths() {
    let db = hydrocube::db_manager::DbManager::open_in_memory().unwrap();
    let cfg: hydrocube::config::CubeConfig = serde_yaml::from_str(
        r#"
name: t
tables:
  - name: greeks
    mode: reference
    schema:
      columns:
        - { name: symbol, type: VARCHAR }
sources:
  - name: sod_greeks
    type: file
    table: greeks
    format: parquet
    load: startup
    path: /data/2026/04/greeks.parquet
window: { interval_ms: 1000 }
persistence: { enabled: false, path: ":memory:", flush_interval: 10 }
aggregation:
  key_columns: [symbol]
  publish:
    sql: "SELECT symbol FROM greeks"
"#,
    )
    .unwrap();

    let mut runner = HookRunner::new(cfg, db);
    runner.resolve_file_paths().unwrap();

    assert_eq!(
        runner.resolved_paths.get("sod_greeks"),
        Some(&vec!["/data/2026/04/greeks.parquet".to_owned()])
    );
}

fn make_cfg_with_startup(startup_sql: &str) -> hydrocube::config::CubeConfig {
    serde_yaml::from_str(&format!(
        r#"
name: test
tables:
  - name: trades
    mode: append
    schema:
      columns:
        - {{ name: book,     type: VARCHAR }}
        - {{ name: notional, type: DOUBLE }}
sources: []
window: {{ interval_ms: 1000 }}
persistence: {{ enabled: false, path: ":memory:", flush_interval: 10 }}
aggregation:
  key_columns: [book]
  startup:
    sql: |
      {startup_sql}
  publish:
    sql: "SELECT book, SUM(notional) AS total FROM {{{{trades}}}} GROUP BY book"
"#
    ))
    .unwrap()
}

fn make_cfg_no_startup() -> hydrocube::config::CubeConfig {
    serde_yaml::from_str(
        r#"
name: t
tables:
  - name: ev
    mode: append
    schema:
      columns:
        - { name: x, type: VARCHAR }
sources: []
window: { interval_ms: 1000 }
persistence: { enabled: false, path: ":memory:", flush_interval: 10 }
aggregation:
  key_columns: [x]
  publish:
    sql: "SELECT x FROM {ev} GROUP BY x"
"#,
    )
    .unwrap()
}

#[tokio::test]
async fn test_startup_sql_runs_against_db() {
    let db = DbManager::open_in_memory().unwrap();
    db.execute(
        "CREATE TABLE IF NOT EXISTS trades (book VARCHAR, notional DOUBLE, _window_id UBIGINT NOT NULL)",
        vec![],
    ).await.unwrap();

    let cfg = make_cfg_with_startup("CREATE TABLE IF NOT EXISTS sod (book VARCHAR, total DOUBLE)");
    let runner = HookRunner::new(cfg, db);
    runner.run_startup().await.unwrap();
    // If startup SQL ran, sod table should exist
    runner
        .db()
        .execute("INSERT INTO sod VALUES ('EMEA', 1000.0)", vec![])
        .await
        .unwrap();
}

#[tokio::test]
async fn test_startup_with_no_hook_is_noop() {
    let db = DbManager::open_in_memory().unwrap();
    let cfg = make_cfg_no_startup();
    let runner = HookRunner::new(cfg, db);
    // Should succeed even with no startup hook
    runner.run_startup().await.unwrap();
}

#[tokio::test]
async fn test_publish_sql_is_expanded() {
    let db = DbManager::open_in_memory().unwrap();
    let cfg = make_cfg_no_startup();
    let runner = HookRunner::new(cfg, db);
    let sql = runner.publish_sql_expanded();
    // {ev} should be expanded (since trades mode is append, it gets a WHERE clause)
    assert!(
        sql.contains("_window_id"),
        "publish SQL should have table expansion, got: {sql}"
    );
}

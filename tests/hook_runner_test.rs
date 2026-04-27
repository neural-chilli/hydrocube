use hydrocube::db_manager::DbManager;
use hydrocube::hooks::runner::HookRunner;
use hydrocube::startup::run_startup_sequence;

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

#[tokio::test]
async fn test_startup_hook_called_during_engine_init() {
    let db = DbManager::open_in_memory().unwrap();

    let cfg: hydrocube::config::CubeConfig = serde_yaml::from_str(
        r#"
name: test
tables:
  - name: trades
    mode: append
    schema:
      columns:
        - { name: book, type: VARCHAR }
        - { name: notional, type: DOUBLE }
sources: []
window: { interval_ms: 1000 }
persistence: { enabled: false, path: ":memory:", flush_interval: 10 }
aggregation:
  key_columns: [book]
  startup:
    sql: "CREATE TABLE IF NOT EXISTS startup_marker (ran BOOLEAN)"
  publish:
    sql: "SELECT book, SUM(notional) AS total FROM trades GROUP BY book"
"#,
    )
    .unwrap();

    // Run the engine initialisation sequence — the same path main.rs calls.
    run_startup_sequence(&db, &cfg).await.unwrap();

    // The startup SQL should have created startup_marker; prove it by inserting.
    db.execute("INSERT INTO startup_marker VALUES (true)", vec![])
        .await
        .expect("startup_marker table must exist after engine init");
}

#[tokio::test]
async fn test_snapshot_cron_fires_and_executes_sql() {
    use hydrocube::hooks::cron::spawn_snapshot_cron_tasks;
    use tokio::sync::watch;

    let db = DbManager::open_in_memory().unwrap();

    let cfg: hydrocube::config::CubeConfig = serde_yaml::from_str(
        r#"
name: test
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
  snapshots:
    - name: hourly
      schedule: "* * * * * *"
      sql: |
        CREATE TABLE IF NOT EXISTS snap_ran (ts BIGINT);
        INSERT INTO snap_ran VALUES (epoch_ms(now()))
  publish:
    sql: "SELECT x FROM ev GROUP BY x"
"#,
    )
    .unwrap();

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let handles = spawn_snapshot_cron_tasks(&cfg, db.clone(), shutdown_rx);

    // Wait 1.5 s — schedule fires every second so at least one execution.
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    let _ = shutdown_tx.send(true);
    for h in handles {
        let _ = h.await;
    }

    let rows = db
        .query_json("SELECT COUNT(*) AS n FROM snap_ran", vec![])
        .await
        .expect("snap_ran must exist after cron fires");
    let n = rows[0]["n"].as_i64().unwrap_or(0);
    assert!(n >= 1, "snapshot cron must have fired at least once, got {n}");
}

#[tokio::test]
async fn test_housekeeping_cron_fires_and_executes_sql() {
    use hydrocube::hooks::cron::spawn_housekeeping_cron_tasks;
    use tokio::sync::watch;

    let db = DbManager::open_in_memory().unwrap();

    let cfg: hydrocube::config::CubeConfig = serde_yaml::from_str(
        r#"
name: test
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
  housekeeping:
    - name: cleanup
      schedule: "* * * * * *"
      sql: |
        CREATE TABLE IF NOT EXISTS hk_ran (ts BIGINT);
        INSERT INTO hk_ran VALUES (epoch_ms(now()))
  publish:
    sql: "SELECT x FROM ev GROUP BY x"
"#,
    )
    .unwrap();

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let handles = spawn_housekeeping_cron_tasks(&cfg, db.clone(), shutdown_rx);

    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    let _ = shutdown_tx.send(true);
    for h in handles {
        let _ = h.await;
    }

    let rows = db
        .query_json("SELECT COUNT(*) AS n FROM hk_ran", vec![])
        .await
        .expect("hk_ran must exist after housekeeping cron fires");
    let n = rows[0]["n"].as_i64().unwrap_or(0);
    assert!(n >= 1, "housekeeping cron must have fired at least once, got {n}");
}

#[tokio::test]
async fn test_reset_hook_executes_sql_and_repopulates_cache() {
    use hydrocube::startup::run_reset_sequence;

    let db = DbManager::open_in_memory().unwrap();

    // Pre-create the trades table and seed rows so the cache population has data.
    db.execute(
        "CREATE TABLE trades (trade_id VARCHAR, book VARCHAR, _window_id UBIGINT)",
        vec![],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO trades VALUES ('R001', 'EMEA', 1), ('R002', 'APAC', 1)",
        vec![],
    )
    .await
    .unwrap();

    let cfg: hydrocube::config::CubeConfig = serde_yaml::from_str(
        r#"
name: test
tables:
  - name: trades
    mode: append
    schema:
      columns:
        - { name: trade_id, type: VARCHAR }
        - { name: book,     type: VARCHAR }
sources:
  - type: http
    table: trades
    identity_key: trade_id
window: { interval_ms: 1000 }
persistence: { enabled: false, path: ":memory:", flush_interval: 10 }
aggregation:
  key_columns: [book]
  reset:
    schedule: "0 0 * * *"
    sql: "CREATE TABLE IF NOT EXISTS reset_marker (ran BOOLEAN)"
  publish:
    sql: "SELECT book FROM trades GROUP BY book"
"#,
    )
    .unwrap();

    let cache = run_reset_sequence(&db, &cfg).await.unwrap();

    // Reset SQL must have run.
    db.execute("INSERT INTO reset_marker VALUES (true)", vec![])
        .await
        .expect("reset_marker must exist after reset sequence");

    // Identity cache must be repopulated from post-reset DB state.
    assert!(cache.seen("trades", "R001"), "R001 must be in cache after reset");
    assert!(cache.seen("trades", "R002"), "R002 must be in cache after reset");
}

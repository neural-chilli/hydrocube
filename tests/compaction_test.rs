// tests/compaction_test.rs

use hydrocube::db_manager::DbManager;
use hydrocube::persistence;
use hydrocube::retention::RetentionManager;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn minimal_cfg_for_persistence() -> hydrocube::config::CubeConfig {
    serde_yaml::from_str(r#"
name: test_cube
tables:
  - name: slices
    mode: append
    schema:
      columns:
        - { name: trade_id, type: VARCHAR }
        - { name: quantity,  type: DOUBLE }
sources: []
window: { interval_ms: 1000 }
persistence: { enabled: true, path: ":memory:", flush_interval: 10 }
aggregation:
  key_columns: [trade_id]
  publish:
    sql: "SELECT trade_id, SUM(quantity) AS qty FROM {slices} GROUP BY trade_id"
"#).unwrap()
}

fn open_db() -> DbManager {
    DbManager::open_in_memory().expect("open in-memory DB")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_parquet_export() {
    let db = open_db();
    let config = minimal_cfg_for_persistence();

    // Initialise tables (creates slices + metadata).
    persistence::init(&db, &config)
        .await
        .expect("persistence init failed");

    // Insert a few rows across different window IDs.
    db.execute(
        "INSERT INTO slices (trade_id, quantity, _window_id) VALUES ('t1', 10.0, 1), ('t2', 20.0, 2), ('t3', 30.0, 5)",
        vec![],
    )
    .await
    .expect("insert slices");

    // Create a temp directory for the Parquet output.
    let parquet_dir = tempfile::tempdir().expect("create temp dir");
    let parquet_path = parquet_dir.path().to_str().expect("valid path");

    // Export windows up to cutoff=3 (should capture t1 and t2, not t3).
    RetentionManager::export_to_parquet(&db, parquet_path, 3)
        .await
        .expect("export_to_parquet failed");

    // Verify the parquet file was created under a date-named subdirectory.
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let expected_file = format!("{}/{}/window_{:06}.parquet", parquet_path, today, 3u64);

    assert!(
        std::path::Path::new(&expected_file).exists(),
        "expected parquet file not found: {}",
        expected_file
    );
}

#[tokio::test]
async fn test_retention_prune_old_files() {
    let base_dir = tempfile::tempdir().expect("create temp dir");
    let base_path = base_dir.path().to_str().expect("valid path");

    // Create an "old" directory (5 days ago) and a "new" directory (today).
    let now = chrono::Utc::now().date_naive();
    let old_date = (now - chrono::Duration::days(5))
        .format("%Y-%m-%d")
        .to_string();
    let new_date = now.format("%Y-%m-%d").to_string();

    let old_dir = format!("{}/{}", base_path, old_date);
    let new_dir = format!("{}/{}", base_path, new_date);

    std::fs::create_dir_all(&old_dir).expect("create old dir");
    std::fs::create_dir_all(&new_dir).expect("create new dir");

    // Also place a dummy file inside each so the directories are non-empty.
    std::fs::write(format!("{}/window_000001.parquet", old_dir), b"dummy")
        .expect("write old parquet");
    std::fs::write(format!("{}/window_000002.parquet", new_dir), b"dummy")
        .expect("write new parquet");

    // Prune with 1-day retention (86400 seconds).
    RetentionManager::prune(base_path, 86400).expect("prune failed");

    // Old directory (5 days ago) should have been deleted.
    assert!(
        !std::path::Path::new(&old_dir).exists(),
        "old directory should have been pruned"
    );

    // New directory (today) should still exist.
    assert!(
        std::path::Path::new(&new_dir).exists(),
        "new directory should not have been pruned"
    );
}

#[tokio::test]
async fn test_compaction_advances_cutoff_without_deleting_rows() {
    use hydrocube::aggregation::window::set_compaction_cutoff;
    use hydrocube::db_manager::DbManager;
    use hydrocube::hooks::runner::HookRunner;

    set_compaction_cutoff(0);

    let db = DbManager::open_in_memory().unwrap();
    db.execute(
        "CREATE TABLE trades (book VARCHAR, notional DOUBLE, _window_id UBIGINT)",
        vec![],
    ).await.unwrap();

    // Insert rows in windows 1 and 2
    for wid in [1u64, 2u64] {
        db.execute(
            &format!("INSERT INTO trades VALUES ('EMEA', 1000.0, {wid})"),
            vec![],
        ).await.unwrap();
    }

    let cfg: hydrocube::config::CubeConfig = serde_yaml::from_str(r#"
name: t
tables:
  - name: trades
    mode: append
    schema:
      columns:
        - { name: book,     type: VARCHAR }
        - { name: notional, type: DOUBLE }
sources: []
window: { interval_ms: 1000 }
persistence: { enabled: false, path: ":memory:", flush_interval: 10 }
aggregation:
  key_columns: [book]
  compaction:
    interval: 60s
    sql: |
      CREATE TABLE IF NOT EXISTS intraday_compacted AS
        SELECT book, SUM(notional) AS total
        FROM (SELECT * FROM trades WHERE _window_id > 0 AND _window_id <= 2)
        GROUP BY book
  publish:
    sql: "SELECT book, SUM(notional) AS total FROM {trades} GROUP BY book"
"#).unwrap();

    let runner = HookRunner::new(cfg, db.clone());

    // Run compaction with new_cutoff = 2
    runner.run_compaction(2).await.unwrap();

    // Advance the cutoff
    hydrocube::aggregation::window::set_compaction_cutoff(2);

    // Raw rows must still be there (no-delete model)
    let rows = db.query_json("SELECT COUNT(*) AS cnt FROM trades", vec![]).await.unwrap();
    let cnt = rows[0]["cnt"].as_u64().unwrap_or(0);
    assert_eq!(cnt, 2, "raw rows must not be deleted by compaction");

    // Compacted table should have been created by the hook SQL
    let agg = db.query_json("SELECT total FROM intraday_compacted WHERE book = 'EMEA'", vec![]).await.unwrap();
    assert!(!agg.is_empty(), "compaction SQL should have created intraday_compacted");
    assert_eq!(agg[0]["total"].as_f64().unwrap(), 2000.0);
}

// tests/compaction_test.rs

use hydrocube::config::{
    AggregationConfig, ColumnDef, CompactionConfig, CubeConfig, PersistenceConfig, RetentionConfig,
    SchemaConfig, SourceConfig, WindowConfig,
};
use hydrocube::db_manager::DbManager;
use hydrocube::persistence;
use hydrocube::retention::RetentionManager;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a minimal CubeConfig suitable for compaction tests.
fn test_config() -> CubeConfig {
    CubeConfig {
        name: "test_cube".into(),
        description: None,
        source: SourceConfig {
            source_type: "test".into(),
            brokers: None,
            topic: None,
            group_id: None,
            format: "json".into(),
        },
        schema: SchemaConfig {
            columns: vec![
                ColumnDef {
                    name: "trade_id".into(),
                    col_type: "VARCHAR".into(),
                },
                ColumnDef {
                    name: "quantity".into(),
                    col_type: "DOUBLE".into(),
                },
            ],
        },
        transform: None,
        aggregation: AggregationConfig {
            sql: "SELECT SUM(quantity) FROM slices".into(),
        },
        window: WindowConfig { interval_ms: 1000 },
        compaction: CompactionConfig {
            interval_windows: 60,
        },
        retention: RetentionConfig {
            duration: "1d".into(),
            parquet_path: "/tmp/test_parquet".into(),
        },
        persistence: PersistenceConfig {
            enabled: true,
            path: ":memory:".into(),
            flush_interval: 10,
        },
        publish: None,
        ui: None,
        auth: None,
        log_level: "info".into(),
    }
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
    let config = test_config();

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

// tests/persistence_test.rs

use hydrocube::config::{
    AggregationConfig, ColumnDef, CompactionConfig, CubeConfig, PersistenceConfig, RetentionConfig,
    SchemaConfig, SourceConfig, WindowConfig,
};
use hydrocube::db_manager::DbManager;
use hydrocube::error::HcError;
use hydrocube::persistence;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a minimal CubeConfig suitable for persistence tests.
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
                ColumnDef {
                    name: "price".into(),
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

/// Returns an in-memory DbManager.
fn open_db() -> DbManager {
    DbManager::open_in_memory().expect("open in-memory DB")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_init_creates_tables() {
    let db = open_db();
    let config = test_config();

    persistence::init(&db, &config).await.expect("init failed");

    // Verify all three tables exist by querying them.
    db.query_json("SELECT * FROM slices LIMIT 0", vec![])
        .await
        .expect("slices table should exist");
    db.query_json("SELECT * FROM consolidated LIMIT 0", vec![])
        .await
        .expect("consolidated table should exist");
    db.query_json("SELECT * FROM _cube_metadata LIMIT 0", vec![])
        .await
        .expect("_cube_metadata table should exist");

    // Verify slices has _window_id column.
    // We insert a row with all schema columns + _window_id to confirm the shape.
    db.execute(
        "INSERT INTO slices (trade_id, quantity, price, _window_id) VALUES ('t1', 1.0, 2.0, 42)",
        vec![],
    )
    .await
    .expect("should be able to insert into slices with _window_id");

    let rows = db
        .query_json(
            "SELECT _window_id FROM slices WHERE trade_id = 't1'",
            vec![],
        )
        .await
        .expect("query slices");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["_window_id"].as_u64(), Some(42));

    // Verify _cube_metadata contains the config hash.
    let meta = db
        .query_json("SELECT config_hash FROM _cube_metadata", vec![])
        .await
        .expect("query metadata");
    assert!(!meta.is_empty(), "_cube_metadata should have a row");
    let stored_hash = meta[0]["config_hash"]
        .as_str()
        .expect("config_hash should be a string");
    assert_eq!(
        stored_hash,
        config.schema_hash(),
        "stored hash must match config.schema_hash()"
    );
}

#[tokio::test]
async fn test_config_hash_mismatch_detected() {
    let db = open_db();
    let config_a = test_config();

    // Initialise with config A.
    persistence::init(&db, &config_a)
        .await
        .expect("init A failed");

    // Build config B with a different schema column.
    let mut config_b = test_config();
    config_b.schema.columns.push(ColumnDef {
        name: "extra_col".into(),
        col_type: "INTEGER".into(),
    });

    // verify_config_hash should detect the mismatch.
    let result = persistence::verify_config_hash(&db, &config_b).await;
    assert!(
        matches!(result, Err(HcError::ConfigHashMismatch)),
        "expected ConfigHashMismatch, got {:?}",
        result
    );
}

#[tokio::test]
async fn test_verify_config_hash_ok() {
    let db = open_db();
    let config = test_config();

    persistence::init(&db, &config).await.expect("init failed");

    // Same config — should succeed.
    persistence::verify_config_hash(&db, &config)
        .await
        .expect("hash should match");
}

#[tokio::test]
async fn test_load_and_save_window_id() {
    let db = open_db();
    let config = test_config();

    persistence::init(&db, &config).await.expect("init failed");

    // Initial value should be 0.
    let initial = persistence::load_last_window_id(&db)
        .await
        .expect("load initial window id");
    assert_eq!(initial, 0);

    // Save a new value.
    persistence::save_window_id(&db, 99)
        .await
        .expect("save window id");

    let loaded = persistence::load_last_window_id(&db)
        .await
        .expect("load window id after save");
    assert_eq!(loaded, 99);
}

#[tokio::test]
async fn test_load_and_save_compaction_cutoff() {
    let db = open_db();
    let config = test_config();

    persistence::init(&db, &config).await.expect("init failed");

    let initial = persistence::load_compaction_cutoff(&db)
        .await
        .expect("load initial cutoff");
    assert_eq!(initial, 0);

    persistence::save_compaction_cutoff(&db, 7)
        .await
        .expect("save cutoff");

    let loaded = persistence::load_compaction_cutoff(&db)
        .await
        .expect("load cutoff after save");
    assert_eq!(loaded, 7);
}

#[tokio::test]
async fn test_reset_drops_and_reinits() {
    let db = open_db();
    let config = test_config();

    persistence::init(&db, &config).await.expect("init failed");

    // Insert a row into slices so we can confirm it is gone after reset.
    db.execute(
        "INSERT INTO slices (trade_id, quantity, price, _window_id) VALUES ('x', 1.0, 1.0, 1)",
        vec![],
    )
    .await
    .expect("insert");

    persistence::reset(&db, &config)
        .await
        .expect("reset failed");

    let rows = db
        .query_json("SELECT * FROM slices", vec![])
        .await
        .expect("slices should exist after reset");
    assert!(rows.is_empty(), "slices should be empty after reset");

    let meta = db
        .query_json("SELECT config_hash FROM _cube_metadata", vec![])
        .await
        .expect("metadata should exist after reset");
    assert!(!meta.is_empty());
}

#[tokio::test]
async fn test_rebuild_best_effort_no_parquet() {
    let db = open_db();
    let config = test_config();

    persistence::init(&db, &config).await.expect("init failed");

    // rebuild should succeed even with no Parquet files present.
    persistence::rebuild(&db, &config)
        .await
        .expect("rebuild should not error on missing parquet files");

    // Tables should exist and metadata should be fresh.
    let meta = db
        .query_json("SELECT config_hash FROM _cube_metadata", vec![])
        .await
        .expect("metadata after rebuild");
    assert!(!meta.is_empty());
    let stored_hash = meta[0]["config_hash"].as_str().unwrap();
    assert_eq!(stored_hash, config.schema_hash());
}

#[tokio::test]
async fn test_init_is_idempotent() {
    let db = open_db();
    let config = test_config();

    // Calling init twice should not error (CREATE TABLE IF NOT EXISTS).
    persistence::init(&db, &config).await.expect("first init");
    persistence::init(&db, &config)
        .await
        .expect("second init (idempotent)");

    // Only one metadata row should exist.
    let rows = db
        .query_json("SELECT COUNT(*) AS cnt FROM _cube_metadata", vec![])
        .await
        .expect("count metadata rows");
    let cnt = rows[0]["cnt"].as_u64().unwrap_or(0);
    assert_eq!(cnt, 1, "exactly one metadata row expected after two inits");
}

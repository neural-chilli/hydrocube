// tests/persistence_test.rs

use hydrocube::config::{
    AggregationConfig, ColumnDef, CubeConfig, DeltaConfig, DrillThroughConfig, PersistenceConfig,
    PublishHookConfig, SchemaConfig, TableConfig, TableMode, WindowConfig,
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
        tables: vec![TableConfig {
            name: "slices".into(),
            mode: TableMode::Append,
            event_time_column: None,
            key_columns: None,
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
        }],
        sources: vec![],
        window: WindowConfig { interval_ms: 1000 },
        persistence: PersistenceConfig {
            enabled: true,
            path: ":memory:".into(),
            flush_interval: 10,
        },
        retention: None,
        drillthrough: DrillThroughConfig::default(),
        delta: DeltaConfig::default(),
        aggregation: AggregationConfig {
            key_columns: vec!["trade_id".into()],
            dimensions: None,
            measures: None,
            startup: None,
            compaction: None,
            publish: PublishHookConfig {
                sql: "SELECT trade_id, SUM(quantity) FROM {slices} GROUP BY trade_id".into(),
            },
            snapshots: None,
            reset: None,
            housekeeping: None,
            reaggregation: None,
        },
        publish: None,
        auth: None,
        peers: None,
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
    config_b.tables[0].schema.columns.push(ColumnDef {
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

// ---------------------------------------------------------------------------
// New multi-table init tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_init_tables_creates_append_table_with_window_id() {
    use hydrocube::config::{ColumnDef, SchemaConfig, TableConfig, TableMode};
    let db = open_db();
    let tables = vec![TableConfig {
        name: "events".into(),
        mode: TableMode::Append,
        event_time_column: None,
        key_columns: None,
        schema: SchemaConfig {
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    col_type: "VARCHAR".into(),
                },
                ColumnDef {
                    name: "val".into(),
                    col_type: "DOUBLE".into(),
                },
            ],
        },
    }];
    persistence::init_tables(&db, &tables).await.unwrap();
    // Insert a row including _window_id
    db.execute(
        "INSERT INTO events (id, val, _window_id) VALUES ('a', 1.0, 1)",
        vec![],
    )
    .await
    .unwrap();
    let rows = db.query_json("SELECT * FROM events", vec![]).await.unwrap();
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn test_init_tables_creates_replace_table_with_primary_key() {
    use hydrocube::config::{ColumnDef, SchemaConfig, TableConfig, TableMode};
    let db = open_db();
    let tables = vec![TableConfig {
        name: "md".into(),
        mode: TableMode::Replace,
        event_time_column: None,
        key_columns: Some(vec!["id".into()]),
        schema: SchemaConfig {
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    col_type: "VARCHAR".into(),
                },
                ColumnDef {
                    name: "val".into(),
                    col_type: "DOUBLE".into(),
                },
            ],
        },
    }];
    persistence::init_tables(&db, &tables).await.unwrap();
    // Duplicate key insert should either error or replace — DuckDB uses INSERT OR REPLACE behaviour
    db.execute("INSERT INTO md (id, val) VALUES ('x', 1.0)", vec![])
        .await
        .unwrap();
    // Verify the table exists and has the right shape
    let rows = db
        .query_json("SELECT val FROM md WHERE id = 'x'", vec![])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["val"].as_f64().unwrap(), 1.0);
}

#[tokio::test]
async fn test_init_tables_creates_reference_table_no_window_id() {
    use hydrocube::config::{ColumnDef, SchemaConfig, TableConfig, TableMode};
    let db = open_db();
    let tables = vec![TableConfig {
        name: "ref_data".into(),
        mode: TableMode::Reference,
        event_time_column: None,
        key_columns: None,
        schema: SchemaConfig {
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    col_type: "VARCHAR".into(),
                },
                ColumnDef {
                    name: "val".into(),
                    col_type: "DOUBLE".into(),
                },
            ],
        },
    }];
    persistence::init_tables(&db, &tables).await.unwrap();
    db.execute("INSERT INTO ref_data (id, val) VALUES ('a', 1.0)", vec![])
        .await
        .unwrap();
    // Confirm no _window_id column (inserting it should fail)
    let result = db
        .execute(
            "INSERT INTO ref_data (id, val, _window_id) VALUES ('b', 2.0, 99)",
            vec![],
        )
        .await;
    assert!(
        result.is_err(),
        "reference table should not have _window_id"
    );
}

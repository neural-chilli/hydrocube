// tests/integration_test.rs
//
// End-to-end integration tests for the full HydroCube pipeline
// (without Kafka).

use hydrocube::config::{
    AggregationConfig, ColumnDef, CompactionConfig, CubeConfig, PersistenceConfig, RetentionConfig,
    SchemaConfig, SourceConfig, WindowConfig,
};
use hydrocube::db_manager::DbManager;
use hydrocube::delta::DeltaDetector;
use hydrocube::error::HcError;
use hydrocube::persistence;
use hydrocube::publish::batch_to_base64_arrow;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a CubeConfig mirroring cube.example.yaml.
fn trading_config() -> CubeConfig {
    CubeConfig {
        name: "trading_positions".into(),
        description: Some("Real-time position aggregation by book".into()),
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
                    name: "book".into(),
                    col_type: "VARCHAR".into(),
                },
                ColumnDef {
                    name: "desk".into(),
                    col_type: "VARCHAR".into(),
                },
                ColumnDef {
                    name: "instrument".into(),
                    col_type: "VARCHAR".into(),
                },
                ColumnDef {
                    name: "instrument_type".into(),
                    col_type: "VARCHAR".into(),
                },
                ColumnDef {
                    name: "currency".into(),
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
                ColumnDef {
                    name: "notional".into(),
                    col_type: "DOUBLE".into(),
                },
                ColumnDef {
                    name: "side".into(),
                    col_type: "VARCHAR".into(),
                },
                ColumnDef {
                    name: "trade_time".into(),
                    col_type: "TIMESTAMP".into(),
                },
            ],
        },
        transform: None,
        aggregation: AggregationConfig {
            sql: "SELECT book, desk, instrument_type, currency, \
                  SUM(notional) AS total_notional, \
                  SUM(CASE WHEN side = 'BUY' THEN quantity ELSE -quantity END) AS net_quantity, \
                  COUNT(*) AS trade_count, \
                  AVG(price) AS avg_price, \
                  MAX(trade_time) AS max_trade_time \
                  FROM slices \
                  GROUP BY book, desk, instrument_type, currency"
                .into(),
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
            enabled: false,
            path: ":memory:".into(),
            flush_interval: 10,
        },
        publish: None,
        ui: None,
        auth: None,
        log_level: "info".into(),
    }
}

/// Open and initialise a fresh in-memory database.
async fn setup_db(config: &CubeConfig) -> DbManager {
    let db = DbManager::open_in_memory().expect("open in-memory DB");
    persistence::init(&db, config)
        .await
        .expect("persistence init");
    db
}

/// Insert 3 test trades into slices for the given window_id.
async fn insert_test_trades(db: &DbManager, window_id: u64) {
    let sql = format!(
        "INSERT INTO slices \
         (trade_id, book, desk, instrument, instrument_type, currency, \
          quantity, price, notional, side, trade_time, _window_id) VALUES \
         ('T1', 'FX',    'Trading', 'EURUSD', 'Spot', 'EUR', 100.0, 1.10, 110.0,   'BUY',  '2024-01-01 10:00:00', {wid}), \
         ('T2', 'FX',    'Trading', 'EURUSD', 'Spot', 'EUR',  50.0, 1.11,  55.5,   'SELL', '2024-01-01 10:00:01', {wid}), \
         ('T3', 'Rates', 'Trading', 'UST10Y', 'Bond', 'USD', 200.0, 98.5,  19700.0,'BUY',  '2024-01-01 10:00:02', {wid})",
        wid = window_id
    );
    db.execute(&sql, vec![])
        .await
        .expect("insert_test_trades failed");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Full end-to-end pipeline test without Kafka.
#[tokio::test]
async fn test_end_to_end_pipeline() {
    let config = trading_config();
    let db = setup_db(&config).await;

    // Step 4: Insert 3 test trades with window_id=1.
    insert_test_trades(&db, 1).await;

    // Step 5: Run aggregation SQL via query_arrow.
    let batches = db
        .query_arrow(&config.aggregation.sql)
        .await
        .expect("query_arrow failed");

    // Step 6: Assert > 0 rows.
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(
        total_rows > 0,
        "expected > 0 aggregated rows, got {}",
        total_rows
    );
    assert_eq!(total_rows, 2, "expected 2 groups (FX, Rates)");

    // Step 7: Create DeltaDetector.
    let dimension_names = vec![
        "book".to_string(),
        "desk".to_string(),
        "instrument_type".to_string(),
        "currency".to_string(),
    ];
    let mut detector = DeltaDetector::new(dimension_names);

    // Merge all batches into one for the detector.
    // (In practice there is typically one batch from DuckDB, but we handle multi.)
    let schema = batches[0].schema();
    let merged_batch =
        duckdb::arrow::compute::concat_batches(&schema, &batches).expect("concat batches");

    // Step 8: First detect() → all rows should be upserts.
    let (upserts1, deletes1) = detector.detect(&merged_batch);
    assert_eq!(
        upserts1.num_rows(),
        total_rows,
        "first detect: all {} rows should be upserts",
        total_rows
    );
    assert_eq!(deletes1.num_rows(), 0, "first detect: no deletes expected");

    // Step 9: Serialize upserts to base64 Arrow IPC → non-empty string.
    let b64 = batch_to_base64_arrow(&upserts1).expect("serialize to base64 Arrow IPC");
    assert!(
        !b64.is_empty(),
        "base64 Arrow IPC payload must not be empty"
    );

    // Step 10: Second detect() with same data → 0 upserts.
    let (upserts2, deletes2) = detector.detect(&merged_batch);
    assert_eq!(
        upserts2.num_rows(),
        0,
        "second detect with same data: expected 0 upserts"
    );
    assert_eq!(
        deletes2.num_rows(),
        0,
        "second detect with same data: expected 0 deletes"
    );

    // Step 11: Insert more data with window_id=2 (change notional for FX),
    // then re-aggregate.
    let sql_w2 = "INSERT INTO slices \
         (trade_id, book, desk, instrument, instrument_type, currency, \
          quantity, price, notional, side, trade_time, _window_id) VALUES \
         ('T4', 'FX', 'Trading', 'EURUSD', 'Spot', 'EUR', 50.0, 1.12, 56.0, 'BUY', '2024-01-01 11:00:00', 2)";
    db.execute(sql_w2, vec![])
        .await
        .expect("insert window 2 trades");

    let batches2 = db
        .query_arrow(&config.aggregation.sql)
        .await
        .expect("query_arrow window 2");

    let merged2 = duckdb::arrow::compute::concat_batches(&batches2[0].schema(), &batches2)
        .expect("concat batches window 2");

    // Step 12: Third detect() → FX group changed (more data added), so >= 1 upsert.
    let (upserts3, _deletes3) = detector.detect(&merged2);
    assert!(
        upserts3.num_rows() > 0,
        "third detect: expected >= 1 upsert after adding FX data, got 0"
    );
}

/// Config hash lifecycle: init, verify same, modify, verify mismatch.
#[tokio::test]
async fn test_config_hash_lifecycle() {
    let config = trading_config();
    let db = setup_db(&config).await;

    // Step 2: Verify hash passes with the original config.
    persistence::verify_config_hash(&db, &config)
        .await
        .expect("hash should match original config");

    // Step 3: Modify config's aggregation SQL.
    let mut modified = config.clone();
    modified.aggregation.sql = "SELECT book, COUNT(*) AS cnt FROM slices GROUP BY book".into();

    // Step 4: Verify hash fails with ConfigHashMismatch.
    let result = persistence::verify_config_hash(&db, &modified).await;
    assert!(
        matches!(result, Err(HcError::ConfigHashMismatch)),
        "expected ConfigHashMismatch after SQL change, got: {:?}",
        result
    );
}

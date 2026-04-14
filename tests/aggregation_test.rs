// tests/aggregation_test.rs
//
// Integration tests for the aggregation hot path: slice insertion,
// full aggregation SQL, and Arrow output.

use hydrocube::config::{
    AggregationConfig, ColumnDef, CompactionConfig, CubeConfig, PersistenceConfig, RetentionConfig,
    SchemaConfig, SourceConfig, WindowConfig,
};
use hydrocube::db_manager::DbManager;
use hydrocube::persistence;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a CubeConfig that mirrors the schema in cube.example.yaml.
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
            sql: "SELECT\n  book,\n  desk,\n  instrument_type,\n  currency,\n  \
                  SUM(notional) AS total_notional,\n  \
                  SUM(CASE WHEN side = 'BUY' THEN quantity ELSE -quantity END) AS net_quantity,\n  \
                  COUNT(*) AS trade_count,\n  \
                  AVG(price) AS avg_price,\n  \
                  MAX(trade_time) AS max_trade_time\n\
                  FROM slices\n\
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

/// Open a fresh in-memory DbManager and initialise it with the trading config.
async fn setup_db(config: &CubeConfig) -> DbManager {
    let db = DbManager::open_in_memory().expect("open in-memory DB");
    persistence::init(&db, config)
        .await
        .expect("persistence init failed");
    db
}

/// Insert three test trades into the slices table with the given window_id.
async fn insert_test_trades(db: &DbManager, window_id: u64) {
    let insert = format!(
        "INSERT INTO slices \
         (trade_id, book, desk, instrument, instrument_type, currency, \
          quantity, price, notional, side, trade_time, _window_id) VALUES \
         ('T1', 'FX', 'Trading', 'EURUSD', 'Spot', 'EUR', 100.0, 1.10, 110.0, 'BUY',  '2024-01-01 10:00:00', {wid}),\
         ('T2', 'FX', 'Trading', 'EURUSD', 'Spot', 'EUR',  50.0, 1.11,  55.5, 'SELL', '2024-01-01 10:00:01', {wid}),\
         ('T3', 'Rates', 'Trading', 'UST10Y', 'Bond', 'USD', 200.0, 98.5, 19700.0, 'BUY', '2024-01-01 10:00:02', {wid})",
        wid = window_id
    );
    db.execute(&insert, vec![])
        .await
        .expect("insert test trades failed");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_basic_aggregation_from_slices() {
    let config = trading_config();
    let db = setup_db(&config).await;

    insert_test_trades(&db, 1).await;

    // Run the full aggregation SQL from config.
    let rows = db
        .query_json(&config.aggregation.sql, vec![])
        .await
        .expect("aggregation query failed");

    // Expect 2 groups: (FX, Trading, Spot, EUR) and (Rates, Trading, Bond, USD).
    assert_eq!(
        rows.len(),
        2,
        "expected 2 aggregation groups, got {}: {:?}",
        rows.len(),
        rows
    );

    // Find the FX group.
    let fx_row = rows
        .iter()
        .find(|r| r.get("book").and_then(|v| v.as_str()) == Some("FX"))
        .expect("FX group not found");

    // total_notional = 110 + 55.5 = 165.5
    let total_notional = fx_row["total_notional"]
        .as_f64()
        .expect("total_notional must be a number");
    assert!(
        (total_notional - 165.5).abs() < 1e-9,
        "FX total_notional expected 165.5, got {}",
        total_notional
    );

    // net_quantity = 100 (BUY) + -50 (SELL) = 50
    let net_qty = fx_row["net_quantity"]
        .as_f64()
        .expect("net_quantity must be a number");
    assert!(
        (net_qty - 50.0).abs() < 1e-9,
        "FX net_quantity expected 50.0, got {}",
        net_qty
    );

    // trade_count = 2
    let trade_count = fx_row["trade_count"]
        .as_u64()
        .expect("trade_count must be an integer");
    assert_eq!(trade_count, 2, "FX trade_count expected 2");

    // Find the Rates group.
    let rates_row = rows
        .iter()
        .find(|r| r.get("book").and_then(|v| v.as_str()) == Some("Rates"))
        .expect("Rates group not found");

    // total_notional = 19700
    let rates_notional = rates_row["total_notional"]
        .as_f64()
        .expect("rates total_notional must be a number");
    assert!(
        (rates_notional - 19700.0).abs() < 1e-9,
        "Rates total_notional expected 19700.0, got {}",
        rates_notional
    );
}

#[tokio::test]
async fn test_aggregation_returns_arrow() {
    let config = trading_config();
    let db = setup_db(&config).await;

    insert_test_trades(&db, 1).await;

    // Run the aggregation via the Arrow path.
    let batches = db
        .query_arrow(&config.aggregation.sql)
        .await
        .expect("query_arrow failed");

    // At least one batch must be returned.
    assert!(!batches.is_empty(), "expected at least one RecordBatch");

    // Verify row and column count across all batches.
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 2,
        "expected 2 aggregated rows, got {}",
        total_rows
    );

    // The example aggregation has 4 dimensions + 5 measures = 9 columns.
    let num_cols = batches[0].num_columns();
    assert_eq!(
        num_cols, 9,
        "expected 9 columns (4 dims + 5 measures), got {}",
        num_cols
    );

    // Verify column names are present in the schema.
    let schema = batches[0].schema();
    let col_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    for expected in &[
        "book",
        "desk",
        "instrument_type",
        "currency",
        "total_notional",
        "net_quantity",
        "trade_count",
        "avg_price",
        "max_trade_time",
    ] {
        assert!(
            col_names.contains(expected),
            "column '{}' not found in schema: {:?}",
            expected,
            col_names
        );
    }
}

// tests/engine_test.rs
//
// Integration test for the hot-path engine loop.

use hydrocube::config::DataFormat;
use hydrocube::db_manager::DbManager;
use hydrocube::engine::run_hot_path;
use hydrocube::ingest::RawMessage;
use hydrocube::persistence;
use hydrocube::publish::DeltaEvent;
use hydrocube::web::api::ErrorCounters;
use serde_json::json;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, watch};

/// Build a minimal new-format CubeConfig inline (cube.example.yaml uses the old format).
fn test_config() -> hydrocube::config::CubeConfig {
    serde_yaml::from_str(
        r#"
name: test_cube
tables:
  - name: trades
    mode: append
    schema:
      columns:
        - { name: trade_id,        type: VARCHAR }
        - { name: book,            type: VARCHAR }
        - { name: desk,            type: VARCHAR }
        - { name: instrument,      type: VARCHAR }
        - { name: instrument_type, type: VARCHAR }
        - { name: currency,        type: VARCHAR }
        - { name: quantity,        type: DOUBLE  }
        - { name: price,           type: DOUBLE  }
        - { name: notional,        type: DOUBLE  }
        - { name: side,            type: VARCHAR }
        - { name: trade_time,      type: VARCHAR }
sources: []
window:
  interval_ms: 100
persistence:
  enabled: false
  path: ":memory:"
  flush_interval: 1
aggregation:
  key_columns: [book]
  publish:
    sql: "SELECT book, SUM(notional) AS total FROM trades WHERE _window_id > 0 GROUP BY book"
"#,
    )
    .unwrap()
}

#[test]
fn test_raw_message_has_table_field() {
    let msg = RawMessage {
        table: "trades".to_owned(),
        bytes: b"{}".to_vec(),
        format: DataFormat::Json,
    };
    assert_eq!(msg.table, "trades");
    assert_eq!(msg.bytes, b"{}");
}

#[tokio::test]
async fn test_hot_path_processes_messages() {
    let db = DbManager::open_in_memory().unwrap();
    let config = test_config();

    // Initialise persistence tables (slices, consolidated, _cube_metadata).
    persistence::init(&db, &config).await.unwrap();
    // Initialise per-table DuckDB tables (trades, etc.).
    persistence::init_tables(&db, &config.tables).await.unwrap();

    let (raw_tx, raw_rx) = mpsc::channel::<RawMessage>(100);
    let (broadcast_tx, mut broadcast_rx) = broadcast::channel::<DeltaEvent>(100);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Spawn engine in background.
    let engine_db = db.clone();
    let engine_config = config.clone();
    let handle = tokio::spawn(async move {
        run_hot_path(
            engine_db,
            engine_config,
            raw_rx,
            broadcast_tx,
            shutdown_rx,
            ErrorCounters::new(),
        )
        .await
    });

    // Send two test trade messages.
    let trade1 = json!({
        "trade_id": "T1",
        "book": "Rates_NY",
        "desk": "Rates",
        "instrument": "UST10Y",
        "instrument_type": "Bond",
        "currency": "USD",
        "quantity": 100.0,
        "price": 98.5,
        "notional": 9850.0,
        "side": "BUY",
        "trade_time": "2026-04-14T10:00:00Z"
    });
    let trade2 = json!({
        "trade_id": "T2",
        "book": "Rates_LDN",
        "desk": "Rates",
        "instrument": "GILT10Y",
        "instrument_type": "Bond",
        "currency": "GBP",
        "quantity": 200.0,
        "price": 101.25,
        "notional": 20250.0,
        "side": "SELL",
        "trade_time": "2026-04-14T10:00:01Z"
    });

    raw_tx
        .send(RawMessage {
            table: "trades".to_owned(),
            bytes: serde_json::to_vec(&trade1).unwrap(),
            format: DataFormat::Json,
        })
        .await
        .unwrap();
    raw_tx
        .send(RawMessage {
            table: "trades".to_owned(),
            bytes: serde_json::to_vec(&trade2).unwrap(),
            format: DataFormat::Json,
        })
        .await
        .unwrap();

    // Wait up to 2 seconds for the window flush + delta broadcast.
    let event = tokio::time::timeout(Duration::from_secs(2), broadcast_rx.recv())
        .await
        .expect("Timeout waiting for delta event")
        .expect("Broadcast channel error");

    assert!(event.row_count > 0, "Should have upserts");
    assert!(!event.base64_arrow.is_empty(), "Should have Arrow IPC data");

    // Shut down cleanly.
    let _ = shutdown_tx.send(true);
    let _ = handle.await;
}

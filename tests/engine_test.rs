// tests/engine_test.rs
//
// Integration test for the hot-path engine loop.

use hydrocube::db_manager::DbManager;
use hydrocube::engine::run_hot_path;
use hydrocube::persistence;
use hydrocube::publish::DeltaEvent;
use serde_json::json;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, watch};

/// Build a CubeConfig from the example YAML, overriding timing values for speed.
fn test_config() -> hydrocube::config::CubeConfig {
    let yaml = std::fs::read_to_string("cube.example.yaml")
        .expect("cube.example.yaml must exist at repo root");
    let mut config: hydrocube::config::CubeConfig =
        serde_yaml::from_str(&yaml).expect("cube.example.yaml must be valid");
    // Fast windows for testing — minimum allowed by validate() is 100 ms.
    config.window.interval_ms = 100;
    config.persistence.flush_interval = 1;
    config
}

#[tokio::test]
async fn test_hot_path_processes_messages() {
    let db = DbManager::open_in_memory().unwrap();
    let config = test_config();

    // Initialise persistence tables (slices, consolidated, _cube_metadata).
    persistence::init(&db, &config).await.unwrap();

    let (raw_tx, raw_rx) = mpsc::channel::<Vec<u8>>(100);
    let (broadcast_tx, mut broadcast_rx) = broadcast::channel::<DeltaEvent>(100);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Spawn engine in background.
    let engine_db = db.clone();
    let engine_config = config.clone();
    let handle = tokio::spawn(async move {
        run_hot_path(engine_db, engine_config, raw_rx, broadcast_tx, shutdown_rx).await
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
        .send(serde_json::to_vec(&trade1).unwrap())
        .await
        .unwrap();
    raw_tx
        .send(serde_json::to_vec(&trade2).unwrap())
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

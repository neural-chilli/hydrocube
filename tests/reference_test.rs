// tests/reference_test.rs
use hydrocube::config::CubeConfig;
use hydrocube::db_manager::DbManager;
use hydrocube::ingest::reference::load_reference_tables_at_startup;
use hydrocube::persistence::init_tables;
use std::io::Write;
use tempfile::NamedTempFile;

#[tokio::test]
async fn test_reference_table_loaded_at_startup_from_csv() {
    let mut f = NamedTempFile::new().unwrap();
    writeln!(f, "instrument_id,sector").unwrap();
    writeln!(f, "AAPL,Tech").unwrap();
    writeln!(f, "JPM,Finance").unwrap();
    f.flush().unwrap();

    let yaml = format!(
        r#"
name: test
tables:
  - name: instruments
    mode: reference
    schema:
      columns:
        - {{ name: instrument_id, type: VARCHAR }}
        - {{ name: sector, type: VARCHAR }}
sources:
  - type: file
    path: "{}"
    table: instruments
    format: csv
    refresh: startup
window: {{ interval_ms: 1000 }}
persistence: {{ path: ":memory:", flush_interval: 10 }}
aggregation:
  key_columns: [instrument_id]
  publish:
    sql: "SELECT instrument_id, sector FROM instruments"
"#,
        f.path().display()
    );
    let config: CubeConfig = serde_yaml::from_str(&yaml).unwrap();

    let db = DbManager::open_in_memory().unwrap();
    init_tables(&db, &config.tables).await.unwrap();

    load_reference_tables_at_startup(&db, &config)
        .await
        .expect("startup load should succeed");

    let rows = db
        .query_json("SELECT COUNT(*) AS n FROM instruments", vec![])
        .await
        .unwrap();
    assert_eq!(rows[0]["n"].as_i64().unwrap(), 2);
}

#[tokio::test]
async fn test_reference_table_not_loaded_when_no_file_source() {
    let yaml = r#"
name: test
tables:
  - name: instruments
    mode: reference
    schema:
      columns:
        - { name: instrument_id, type: VARCHAR }
sources: []
window: { interval_ms: 1000 }
persistence: { path: ":memory:", flush_interval: 10 }
aggregation:
  key_columns: [instrument_id]
  publish:
    sql: "SELECT instrument_id FROM instruments"
"#;
    let config: CubeConfig = serde_yaml::from_str(yaml).unwrap();
    let db = DbManager::open_in_memory().unwrap();
    init_tables(&db, &config.tables).await.unwrap();

    load_reference_tables_at_startup(&db, &config)
        .await
        .expect("no-op should succeed");

    let rows = db
        .query_json("SELECT COUNT(*) AS n FROM instruments", vec![])
        .await
        .unwrap();
    assert_eq!(rows[0]["n"].as_i64().unwrap(), 0);
}

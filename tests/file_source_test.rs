use hydrocube::config::DataFormat;
use hydrocube::db_manager::DbManager;
use hydrocube::ingest::file::load_file_into_table;
use std::io::Write;
use tempfile::NamedTempFile;

#[tokio::test]
async fn test_load_jsonlines_file_into_table() {
    let db = DbManager::open_in_memory().unwrap();
    db.execute(
        "CREATE TABLE greeks (symbol VARCHAR, delta DOUBLE, gamma DOUBLE)",
        vec![],
    )
    .await
    .unwrap();

    // Write a JSON Lines temp file
    let mut f = NamedTempFile::new().unwrap();
    writeln!(f, r#"{{"symbol":"AAPL","delta":0.5,"gamma":0.02}}"#).unwrap();
    writeln!(f, r#"{{"symbol":"GOOGL","delta":0.7,"gamma":0.01}}"#).unwrap();
    f.flush().unwrap();

    load_file_into_table(
        &db,
        "greeks",
        f.path().to_str().unwrap(),
        DataFormat::JsonLines,
    )
    .await
    .unwrap();

    let rows = db
        .query_json("SELECT COUNT(*) AS cnt FROM greeks", vec![])
        .await
        .unwrap();
    assert_eq!(rows[0]["cnt"].as_u64().unwrap(), 2);
}

#[tokio::test]
async fn test_load_csv_file_into_table() {
    let db = DbManager::open_in_memory().unwrap();
    db.execute("CREATE TABLE prices (ticker VARCHAR, price DOUBLE)", vec![])
        .await
        .unwrap();

    let mut f = NamedTempFile::new().unwrap();
    writeln!(f, "ticker,price").unwrap();
    writeln!(f, "AAPL,150.0").unwrap();
    writeln!(f, "MSFT,300.0").unwrap();
    f.flush().unwrap();

    load_file_into_table(&db, "prices", f.path().to_str().unwrap(), DataFormat::Csv)
        .await
        .unwrap();

    let rows = db
        .query_json("SELECT COUNT(*) AS cnt FROM prices", vec![])
        .await
        .unwrap();
    assert_eq!(rows[0]["cnt"].as_u64().unwrap(), 2);
}

#[tokio::test]
async fn test_load_nonexistent_file_returns_error() {
    let db = DbManager::open_in_memory().unwrap();
    db.execute("CREATE TABLE t (x VARCHAR)", vec![])
        .await
        .unwrap();
    let result =
        load_file_into_table(&db, "t", "/nonexistent/path/file.json", DataFormat::Json).await;
    assert!(result.is_err(), "should error on missing file");
}

// tests/db_manager_test.rs

use hydrocube::db_manager::DbManager;
use serde_json::Value as JsonValue;

// ---------------------------------------------------------------------------
// test_query_returns_result
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_query_returns_result() {
    let db = DbManager::open_in_memory().expect("open in-memory DB");

    db.execute("CREATE TABLE items (id INTEGER, name TEXT)", vec![])
        .await
        .expect("create table");

    db.execute("INSERT INTO items VALUES (1, 'alpha')", vec![])
        .await
        .expect("insert 1");
    db.execute("INSERT INTO items VALUES (2, 'beta')", vec![])
        .await
        .expect("insert 2");
    db.execute("INSERT INTO items VALUES (3, 'gamma')", vec![])
        .await
        .expect("insert 3");

    let rows = db
        .query_json("SELECT id, name FROM items ORDER BY id", vec![])
        .await
        .expect("select");

    assert_eq!(rows.len(), 3, "expected 3 rows");

    assert_eq!(rows[0]["id"], JsonValue::from(1i64));
    assert_eq!(rows[0]["name"], JsonValue::from("alpha"));

    assert_eq!(rows[1]["id"], JsonValue::from(2i64));
    assert_eq!(rows[1]["name"], JsonValue::from("beta"));

    assert_eq!(rows[2]["id"], JsonValue::from(3i64));
    assert_eq!(rows[2]["name"], JsonValue::from("gamma"));
}

// ---------------------------------------------------------------------------
// test_transaction_atomic_swap
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_transaction_atomic_swap() {
    let db = DbManager::open_in_memory().expect("open in-memory DB");

    // Create the "original" table with value 1.
    db.execute("CREATE TABLE original (val INTEGER)", vec![])
        .await
        .expect("create original");
    db.execute("INSERT INTO original VALUES (1)", vec![])
        .await
        .expect("insert into original");

    // Create the "replacement" table with value 99.
    db.execute("CREATE TABLE replacement (val INTEGER)", vec![])
        .await
        .expect("create replacement");
    db.execute("INSERT INTO replacement VALUES (99)", vec![])
        .await
        .expect("insert into replacement");

    // Atomically drop original and rename replacement → original.
    db.transaction(vec![
        "DROP TABLE original".into(),
        "ALTER TABLE replacement RENAME TO original".into(),
    ])
    .await
    .expect("transaction");

    let rows = db
        .query_json("SELECT val FROM original", vec![])
        .await
        .expect("select after swap");

    assert_eq!(rows.len(), 1, "expected 1 row after swap");
    assert_eq!(rows[0]["val"], JsonValue::from(99i64));
}

// ---------------------------------------------------------------------------
// test_query_arrow
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_query_arrow() {
    let db = DbManager::open_in_memory().expect("open in-memory DB");

    db.execute(
        "CREATE TABLE measurements (sensor TEXT, reading DOUBLE)",
        vec![],
    )
    .await
    .expect("create table");

    db.execute("INSERT INTO measurements VALUES ('A', 1.1)", vec![])
        .await
        .expect("insert A");
    db.execute("INSERT INTO measurements VALUES ('B', 2.2)", vec![])
        .await
        .expect("insert B");
    db.execute("INSERT INTO measurements VALUES ('C', 3.3)", vec![])
        .await
        .expect("insert C");

    let batches = db
        .query_arrow("SELECT sensor, reading FROM measurements ORDER BY sensor")
        .await
        .expect("query_arrow");

    assert!(!batches.is_empty(), "expected at least one RecordBatch");

    // Verify total row count and column count across all batches.
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3, "expected 3 rows total");

    let num_cols = batches[0].num_columns();
    assert_eq!(num_cols, 2, "expected 2 columns");
}

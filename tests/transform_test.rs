// tests/transform_test.rs
//
// Integration tests for the SQL transform pipeline.

use hydrocube::config::ColumnDef;
use hydrocube::db_manager::DbManager;
use hydrocube::transform::sql::SqlTransform;
use serde_json::json;

/// Helper: build a two-column schema with an INTEGER id and a DOUBLE value.
fn two_col_schema() -> Vec<ColumnDef> {
    vec![
        ColumnDef {
            name: "id".to_string(),
            col_type: "INTEGER".to_string(),
        },
        ColumnDef {
            name: "value".to_string(),
            col_type: "DOUBLE".to_string(),
        },
    ]
}

#[tokio::test]
async fn test_sql_transform_filter() {
    // Three rows: (1, 10.0), (2, -5.0), (3, 7.5)
    // SQL filters value > 0 → expect rows 1 and 3.
    let db = DbManager::open_in_memory().expect("open in-memory DB");
    let columns = two_col_schema();

    let input = vec![
        vec![json!(1), json!(10.0)],
        vec![json!(2), json!(-5.0)],
        vec![json!(3), json!(7.5)],
    ];

    let transform =
        SqlTransform::new("SELECT * FROM raw_buffer WHERE value > 0 ORDER BY id".to_string());
    let result = transform
        .execute(&db, &columns, input)
        .await
        .expect("transform should succeed");

    assert_eq!(result.len(), 2, "expected 2 rows after filtering value > 0");

    // Row 0 → id=1, value=10.0
    assert_eq!(result[0][0], json!(1));
    // Row 1 → id=3, value=7.5
    assert_eq!(result[1][0], json!(3));

    db.shutdown().await;
}

#[tokio::test]
async fn test_sql_transform_empty_input() {
    // Empty input must return empty output without touching the DB.
    let db = DbManager::open_in_memory().expect("open in-memory DB");
    let columns = two_col_schema();

    let transform = SqlTransform::new("SELECT * FROM raw_buffer".to_string());
    let result = transform
        .execute(&db, &columns, vec![])
        .await
        .expect("empty input should succeed");

    assert!(result.is_empty(), "expected empty output for empty input");

    db.shutdown().await;
}

#[tokio::test]
async fn test_sql_transform_string_column() {
    // Verify string literals are properly escaped and returned.
    let db = DbManager::open_in_memory().expect("open in-memory DB");
    let columns = vec![
        ColumnDef {
            name: "label".to_string(),
            col_type: "VARCHAR".to_string(),
        },
        ColumnDef {
            name: "score".to_string(),
            col_type: "INTEGER".to_string(),
        },
    ];

    let input = vec![
        vec![json!("alpha"), json!(100)],
        vec![json!("beta"), json!(50)],
        vec![json!("gamma"), json!(75)],
    ];

    let transform = SqlTransform::new(
        "SELECT * FROM raw_buffer WHERE score >= 75 ORDER BY score DESC".to_string(),
    );
    let result = transform
        .execute(&db, &columns, input)
        .await
        .expect("string transform should succeed");

    assert_eq!(result.len(), 2);

    db.shutdown().await;
}

#[tokio::test]
async fn test_sql_transform_null_values() {
    // Rows with NULL values must round-trip correctly.
    let db = DbManager::open_in_memory().expect("open in-memory DB");
    let columns = two_col_schema();

    let input = vec![
        vec![json!(1), serde_json::Value::Null],
        vec![json!(2), json!(3.14)],
    ];

    let transform = SqlTransform::new("SELECT * FROM raw_buffer ORDER BY id".to_string());
    let result = transform
        .execute(&db, &columns, input)
        .await
        .expect("null value transform should succeed");

    assert_eq!(result.len(), 2);
    // First row's value column should be null.
    assert!(result[0][1].is_null(), "expected NULL in first row's value");

    db.shutdown().await;
}

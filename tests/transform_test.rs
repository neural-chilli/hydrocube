// tests/transform_test.rs
//
// Integration tests for the SQL and Lua transform pipelines.

use hydrocube::config::ColumnDef;
use hydrocube::db_manager::DbManager;
use hydrocube::transform::lua::LuaTransform;
use hydrocube::transform::sql::SqlTransform;
use serde_json::json;
use std::sync::Mutex;

// Lua tests must run serially because the vendored Lua C library uses global
// state that is not safe for concurrent multi-threaded access.
static LUA_LOCK: Mutex<()> = Mutex::new(());

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

// ---------------------------------------------------------------------------
// SQL transform column-order regression test (Fix 4)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_sql_transform_preserves_column_order() {
    // Schema: zebra (INTEGER), alpha (INTEGER).
    // SELECT zebra, alpha means the z column must come first in each output row,
    // even though alphabetically alpha < zebra.
    let db = DbManager::open_in_memory().expect("open in-memory DB");
    let columns = vec![
        ColumnDef {
            name: "zebra".to_string(),
            col_type: "INTEGER".to_string(),
        },
        ColumnDef {
            name: "alpha".to_string(),
            col_type: "INTEGER".to_string(),
        },
    ];

    let input = vec![vec![json!(1), json!(2)]]; // zebra=1, alpha=2

    let transform = SqlTransform::new("SELECT zebra, alpha FROM raw_buffer".to_string());
    let result = transform
        .execute(&db, &columns, input)
        .await
        .expect("column-order transform should succeed");

    assert_eq!(result.len(), 1, "expected 1 output row");
    // zebra (value 1) must be first, alpha (value 2) must be second.
    assert_eq!(
        result[0][0],
        json!(1),
        "first column (zebra) must be 1, got {:?}",
        result[0]
    );
    assert_eq!(
        result[0][1],
        json!(2),
        "second column (alpha) must be 2, got {:?}",
        result[0]
    );

    db.shutdown().await;
}

// ---------------------------------------------------------------------------
// Lua transform tests
// ---------------------------------------------------------------------------

fn two_col_names() -> Vec<String> {
    vec!["id".to_string(), "value".to_string()]
}

#[test]
fn test_lua_batch_transform() {
    let _guard = LUA_LOCK.lock().unwrap();

    // Inline Lua: transform_batch doubles positive values, filters negatives.
    let source = r#"
function transform_batch(messages)
    local out = {}
    for _, msg in ipairs(messages) do
        if msg.value > 0 then
            table.insert(out, {id = msg.id, value = msg.value * 2})
        end
    end
    return out
end
"#
    .to_string();

    let col_names = two_col_names();
    let transform = LuaTransform::from_source(source, "transform_batch".to_string(), None)
        .expect("LuaTransform should load");

    // Three rows: (1, 10), (2, -5), (3, 20)
    let input = vec![
        vec![json!(1), json!(10)],
        vec![json!(2), json!(-5)],
        vec![json!(3), json!(20)],
    ];

    let result = transform
        .execute_batch(input, &col_names)
        .expect("batch transform should succeed");

    // Negatives filtered → 2 rows; values doubled → 20, 40
    assert_eq!(result.len(), 2, "expected 2 rows after filtering negatives");

    // Each row has 2 values: id and doubled value.
    // Collect all integer values across all rows and verify 20 and 40 appear.
    let all_values: Vec<i64> = result
        .iter()
        .flat_map(|row| row.iter().filter_map(|v| v.as_i64()))
        .collect();

    assert!(
        all_values.contains(&20),
        "expected doubled value 20 in result, got: {all_values:?}"
    );
    assert!(
        all_values.contains(&40),
        "expected doubled value 40 in result, got: {all_values:?}"
    );
}

#[test]
fn test_lua_per_message_fallback() {
    let _guard = LUA_LOCK.lock().unwrap();

    // Inline Lua: transform(msg) returns a one-element array containing the
    // transformed row table.  {{...}} in Lua means a table of tables.
    let source = r#"
function transform(msg)
    return {{id = msg.id, value = msg.value + 1}}
end
"#
    .to_string();

    let col_names = two_col_names();
    let transform = LuaTransform::from_source(source, "transform".to_string(), None)
        .expect("LuaTransform should load");

    let input = vec![vec![json!(1), json!(9)]];

    let result = transform
        .execute_per_message(input, &col_names)
        .expect("per-message transform should succeed");

    assert_eq!(result.len(), 1, "expected 1 output row");
    // The row has 2 values: id=1 and value=10 (9+1).
    // Assert that 10 appears somewhere in the row values.
    let int_values: Vec<i64> = result[0].iter().filter_map(|v| v.as_i64()).collect();
    assert!(
        int_values.contains(&10),
        "expected incremented value 10 in row, got: {int_values:?}"
    );
}

#[test]
fn test_lua_transform_returns_empty_filters_message() {
    let _guard = LUA_LOCK.lock().unwrap();

    // Inline Lua: transform_batch returns empty table → all messages filtered.
    let source = r#"
function transform_batch(messages)
    return {}
end
"#
    .to_string();

    let col_names = two_col_names();
    let transform = LuaTransform::from_source(source, "transform_batch".to_string(), None)
        .expect("LuaTransform should load");

    let input = vec![vec![json!(1), json!(42)]];

    let result = transform
        .execute_batch(input, &col_names)
        .expect("batch transform should succeed");

    assert!(
        result.is_empty(),
        "expected 0 rows when transform_batch returns empty table"
    );
}

#[test]
fn test_lua_column_order_preserved() {
    let _guard = LUA_LOCK.lock().unwrap();

    // Schema order: ["zebra", "alpha"].
    // Lua returns {zebra=1, alpha=2}.  The output row must be [1, 2] (zebra
    // first), NOT [2, 1] (what you'd get from hash-table iteration order).
    let source = r#"
function transform_batch(messages)
    local out = {}
    for _, msg in ipairs(messages) do
        table.insert(out, {zebra = msg.zebra, alpha = msg.alpha})
    end
    return out
end
"#
    .to_string();

    let col_names = vec!["zebra".to_string(), "alpha".to_string()];
    let transform = LuaTransform::from_source(source, "transform_batch".to_string(), None)
        .expect("LuaTransform should load");

    let input = vec![vec![json!(1), json!(2)]]; // zebra=1, alpha=2

    let result = transform
        .execute_batch(input, &col_names)
        .expect("batch transform should succeed");

    assert_eq!(result.len(), 1, "expected 1 output row");
    // zebra must be first (value 1), alpha must be second (value 2).
    assert_eq!(
        result[0][0],
        json!(1),
        "first column (zebra) must be 1, got {:?}",
        result[0]
    );
    assert_eq!(
        result[0][1],
        json!(2),
        "second column (alpha) must be 2, got {:?}",
        result[0]
    );
}

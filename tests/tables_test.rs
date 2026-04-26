use hydrocube::tables::{AppendBuffer, ReplaceBuffer, TableBuffer};
use serde_json::json;

#[test]
fn test_append_buffer_collects_rows() {
    let mut buf = AppendBuffer::new();
    buf.push(vec![json!("EMEA"), json!(1000.0)]);
    buf.push(vec![json!("EMEA"), json!(500.0)]);
    assert_eq!(buf.len(), 2);
    let rows = buf.drain();
    assert_eq!(rows.len(), 2);
    assert_eq!(buf.len(), 0); // drained
}

#[test]
fn test_replace_buffer_keeps_latest_per_key() {
    let key_cols = vec!["curve".to_owned(), "tenor".to_owned()];
    let schema_cols = vec!["curve".to_owned(), "tenor".to_owned(), "rate".to_owned()];
    let mut buf = ReplaceBuffer::new(key_cols, schema_cols);
    // Insert two rows for the same key — second should win
    buf.upsert(vec![json!("USD"), json!("1Y"), json!(3.5)]);
    buf.upsert(vec![json!("USD"), json!("1Y"), json!(3.6)]);
    // Different key
    buf.upsert(vec![json!("EUR"), json!("1Y"), json!(2.0)]);
    let rows = buf.flush();
    assert_eq!(rows.len(), 2);
    let usd = rows.iter().find(|r| r[0] == json!("USD")).unwrap();
    assert_eq!(usd[2], json!(3.6)); // latest wins
}

#[test]
fn test_table_buffer_enum() {
    let mut buf = TableBuffer::new_append();
    buf.push_append(vec![json!("X"), json!(1.0)]);
    assert_eq!(buf.append_len(), 1);
}

#[test]
fn test_replace_buffer_flush_clears_map() {
    let key_cols = vec!["id".to_owned()];
    let schema_cols = vec!["id".to_owned(), "val".to_owned()];
    let mut buf = ReplaceBuffer::new(key_cols, schema_cols);
    buf.upsert(vec![json!("A"), json!(1.0)]);
    let rows = buf.flush();
    assert_eq!(rows.len(), 1);
    let rows2 = buf.flush();
    assert_eq!(rows2.len(), 0); // cleared after first flush
}

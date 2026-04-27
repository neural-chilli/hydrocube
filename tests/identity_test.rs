use hydrocube::identity::IdentityCache;

#[test]
fn test_seen_returns_false_for_new_key() {
    let cache = IdentityCache::new();
    assert!(!cache.seen("TRADE-001"));
}

#[test]
fn test_mark_seen_then_seen_returns_true() {
    let mut cache = IdentityCache::new();
    cache.mark_seen("TRADE-001");
    assert!(cache.seen("TRADE-001"));
}

#[test]
fn test_multiple_tables() {
    let mut multi = hydrocube::identity::MultiTableIdentityCache::new();
    multi.mark_seen("trades", "TRADE-001");
    multi.mark_seen("trades", "TRADE-002");
    multi.mark_seen("orders", "ORD-001");

    assert!(multi.seen("trades", "TRADE-001"));
    assert!(multi.seen("trades", "TRADE-002"));
    assert!(multi.seen("orders", "ORD-001"));
    assert!(!multi.seen("trades", "ORD-001")); // different table namespace
}

#[test]
fn test_populate_from_strings() {
    let mut cache = IdentityCache::new();
    cache.populate(vec!["A".to_owned(), "B".to_owned(), "C".to_owned()]);
    assert!(cache.seen("A"));
    assert!(cache.seen("B"));
    assert!(!cache.seen("D"));
}

#[test]
fn test_len_and_is_empty() {
    let mut cache = IdentityCache::new();
    assert!(cache.is_empty());
    cache.mark_seen("X");
    assert_eq!(cache.len(), 1);
    assert!(!cache.is_empty());
}

#[tokio::test]
async fn test_populate_from_duckdb() {
    use hydrocube::db_manager::DbManager;
    use hydrocube::identity::populate_identity_cache_from_db;

    let db = DbManager::open_in_memory().unwrap();
    db.execute(
        "CREATE TABLE trades (trade_id VARCHAR, book VARCHAR, _window_id UBIGINT)",
        vec![],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO trades VALUES ('T001', 'EMEA', 1), ('T002', 'APAC', 1)",
        vec![],
    )
    .await
    .unwrap();

    let keys = populate_identity_cache_from_db(&db, "trades", "trade_id")
        .await
        .unwrap();
    assert_eq!(keys.len(), 2);
    assert!(keys.contains(&"T001".to_owned()));
    assert!(keys.contains(&"T002".to_owned()));
}

#[tokio::test]
async fn test_identity_cache_populated_by_startup_sequence() {
    use hydrocube::db_manager::DbManager;
    use hydrocube::startup::run_startup_sequence;

    let db = DbManager::open_in_memory().unwrap();

    // Create the trades table and insert known rows.
    db.execute(
        "CREATE TABLE trades (trade_id VARCHAR, book VARCHAR, _window_id UBIGINT)",
        vec![],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO trades VALUES ('T001', 'EMEA', 1), ('T002', 'APAC', 1)",
        vec![],
    )
    .await
    .unwrap();

    // Config with an HTTP source that declares identity_key: trade_id.
    let cfg: hydrocube::config::CubeConfig = serde_yaml::from_str(
        r#"
name: test
tables:
  - name: trades
    mode: append
    schema:
      columns:
        - { name: trade_id, type: VARCHAR }
        - { name: book,     type: VARCHAR }
sources:
  - type: http
    table: trades
    identity_key: trade_id
window: { interval_ms: 1000 }
persistence: { enabled: false, path: ":memory:", flush_interval: 10 }
aggregation:
  key_columns: [book]
  publish:
    sql: "SELECT book FROM trades GROUP BY book"
"#,
    )
    .unwrap();

    let cache = run_startup_sequence(&db, &cfg).await.unwrap();

    assert!(
        cache.seen("trades", "T001"),
        "T001 must be in cache after startup sequence"
    );
    assert!(
        cache.seen("trades", "T002"),
        "T002 must be in cache after startup sequence"
    );
}

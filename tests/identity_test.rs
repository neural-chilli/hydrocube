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

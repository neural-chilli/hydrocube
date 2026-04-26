use hydrocube::hooks::placeholder::PlaceholderContext;
use hydrocube::config::TableMode;
use chrono::{TimeZone, Utc};

fn ctx_at(y: i32, m: u32, d: u32) -> PlaceholderContext {
    let now = Utc.with_ymd_and_hms(y, m, d, 12, 0, 0).unwrap();
    PlaceholderContext::new(42, None, now, None)
}

fn ctx_with_tables() -> PlaceholderContext {
    let now = Utc::now();
    let mut ctx = PlaceholderContext::new(5, Some(15), now, None);
    ctx.table_modes.insert("trades".to_owned(), TableMode::Append);
    ctx.table_modes.insert("market_data".to_owned(), TableMode::Replace);
    ctx
}

#[test]
fn test_date_tokens_expand() {
    let ctx = ctx_at(2026, 4, 26);
    let sql = "SELECT {YYYY}, {MM}, {DD}, {YYYYMMDD}, {date}";
    let out = ctx.expand(sql);
    assert_eq!(out, "SELECT 2026, 04, 26, 20260426, 2026-04-26");
}

#[test]
fn test_cutoff_expands() {
    let ctx = ctx_at(2026, 4, 26);
    let sql = "WHERE _window_id > {cutoff}";
    let out = ctx.expand(sql);
    assert_eq!(out, "WHERE _window_id > 42");
}

#[test]
fn test_new_cutoff_expands() {
    let now = Utc::now();
    let ctx = PlaceholderContext::new(10, Some(20), now, None);
    assert_eq!(ctx.expand("LIMIT {new_cutoff}"), "LIMIT 20");
}

#[test]
fn test_now_expands() {
    let now = Utc.with_ymd_and_hms(2026, 4, 26, 9, 30, 0).unwrap();
    let ctx = PlaceholderContext::new(0, None, now, None);
    let out = ctx.expand("{now}");
    assert!(out.starts_with("2026-04-26"), "got: {out}");
}

#[test]
fn test_unknown_token_is_left_alone() {
    let ctx = ctx_at(2026, 4, 26);
    let sql = "WHERE foo = {unknown_token}";
    let out = ctx.expand(sql);
    assert_eq!(out, "WHERE foo = {unknown_token}");
}

#[test]
fn test_append_table_token_expands_with_where_clause() {
    let ctx = ctx_with_tables();
    let sql = "SELECT * FROM {trades}";
    let out = ctx.expand(sql);
    assert_eq!(out, "SELECT * FROM (SELECT * FROM trades WHERE _window_id > 5)");
}

#[test]
fn test_replace_table_token_expands_bare() {
    let ctx = ctx_with_tables();
    let sql = "JOIN {market_data} m ON g.curve = m.curve";
    let out = ctx.expand(sql);
    assert_eq!(out, "JOIN market_data m ON g.curve = m.curve");
}

#[test]
fn test_pending_table_token_expands() {
    let ctx = ctx_with_tables();
    let sql = "FROM {pending.trades}";
    let out = ctx.expand(sql);
    assert_eq!(out, "FROM (SELECT * FROM trades WHERE _window_id > 5 AND _window_id <= 15)");
}

#[test]
fn test_path_token_single_path() {
    let now = Utc::now();
    let mut ctx = PlaceholderContext::new(0, None, now, None);
    ctx.resolved_paths.insert("sod_data".to_owned(), vec!["/data/2026/sod.parquet".to_owned()]);
    let out = ctx.expand("read_parquet({path.sod_data})");
    assert_eq!(out, "read_parquet('/data/2026/sod.parquet')");
}

#[test]
fn test_path_token_multiple_paths() {
    let now = Utc::now();
    let mut ctx = PlaceholderContext::new(0, None, now, None);
    ctx.resolved_paths.insert("files".to_owned(), vec![
        "/data/a.parquet".to_owned(),
        "/data/b.parquet".to_owned(),
    ]);
    let out = ctx.expand("read_parquet({path.files})");
    assert_eq!(out, "read_parquet(['/data/a.parquet', '/data/b.parquet'])");
}

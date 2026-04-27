use hydrocube::identity::IdentityCache;
use hydrocube::transform::lua_ctx::run_lua_with_identity_ctx;
use serde_json::json;

#[test]
fn test_lua_ctx_seen_returns_false_for_unknown() {
    let cache = IdentityCache::new();
    let lua_code = r#"
function transform(batch, ctx)
    local out = {}
    for _, row in ipairs(batch) do
        row.is_amendment = ctx.seen(row.trade_id) and 1 or 0
        table.insert(out, row)
    end
    return out
end
"#;
    let rows = vec![vec![json!("T001"), json!("EMEA"), json!(1000.0)]];
    let col_names = vec![
        "trade_id".to_owned(),
        "book".to_owned(),
        "notional".to_owned(),
    ];

    let (out_rows, marks) =
        run_lua_with_identity_ctx(lua_code, "transform", &rows, &col_names, &cache).unwrap();
    assert_eq!(out_rows.len(), 1);
    // No marks yet (didn't call ctx.mark_seen)
    assert!(marks.is_empty());
}

#[test]
fn test_lua_ctx_mark_seen_collected() {
    let cache = IdentityCache::new();
    let lua_code = r#"
function transform(batch, ctx)
    local out = {}
    for _, row in ipairs(batch) do
        ctx.mark_seen(row.trade_id)
        table.insert(out, row)
    end
    return out
end
"#;
    let rows = vec![
        vec![json!("T001"), json!("EMEA"), json!(1000.0)],
        vec![json!("T002"), json!("APAC"), json!(2000.0)],
    ];
    let col_names = vec![
        "trade_id".to_owned(),
        "book".to_owned(),
        "notional".to_owned(),
    ];

    let (_out_rows, marks) =
        run_lua_with_identity_ctx(lua_code, "transform", &rows, &col_names, &cache).unwrap();
    assert_eq!(marks.len(), 2);
    assert!(marks.contains(&"T001".to_owned()));
    assert!(marks.contains(&"T002".to_owned()));
}

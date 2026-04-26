use hydrocube::ingest::file::resolve_paths_with_lua;

#[test]
fn test_inline_lua_path_resolver_returns_paths() {
    let inline_lua = r#"
function resolve_paths()
    return {"/data/2026/04/sod.parquet", "/data/2026/04/extra.parquet"}
end
"#;
    let paths = resolve_paths_with_lua(inline_lua, "resolve_paths").unwrap();
    assert_eq!(paths.len(), 2);
    assert_eq!(paths[0], "/data/2026/04/sod.parquet");
}

#[test]
fn test_lua_resolver_single_path_as_string() {
    let inline_lua = r#"
function get_path()
    return "/data/sod.parquet"
end
"#;
    let paths = resolve_paths_with_lua(inline_lua, "get_path").unwrap();
    assert_eq!(paths.len(), 1);
    assert_eq!(paths[0], "/data/sod.parquet");
}

#[test]
fn test_lua_resolver_with_date_context() {
    // The resolver can use any Lua logic; this simulates date-based path building
    let inline_lua = r#"
function resolve()
    local year = 2026
    local month = 4
    return {string.format("/data/%d/%02d/sod.parquet", year, month)}
end
"#;
    let paths = resolve_paths_with_lua(inline_lua, "resolve").unwrap();
    assert_eq!(paths[0], "/data/2026/04/sod.parquet");
}

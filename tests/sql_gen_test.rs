// tests/sql_gen_test.rs

use hydrocube::aggregation::sql_gen::AggSqlGenerator;

#[test]
fn test_parse_dimensions_from_group_by() {
    let sql = "SELECT book, desk, SUM(notional) AS total FROM slices GROUP BY book, desk";
    let gen = AggSqlGenerator::from_user_sql(sql).unwrap();
    assert_eq!(gen.dimensions(), &["book", "desk"]);
}

#[test]
fn test_parse_measures_from_select() {
    let sql = "SELECT book, SUM(notional) AS total, COUNT(*) AS cnt FROM slices GROUP BY book";
    let gen = AggSqlGenerator::from_user_sql(sql).unwrap();
    assert_eq!(gen.measures().len(), 2);
    assert_eq!(gen.measures()[0].alias, "total");
    assert_eq!(gen.measures()[1].alias, "cnt");
}

#[test]
fn test_generates_slice_aggregation_sql() {
    let sql = "SELECT book, SUM(notional) AS total FROM slices GROUP BY book";
    let gen = AggSqlGenerator::from_user_sql(sql).unwrap();
    let slice_sql = gen.slice_aggregation_sql();
    assert!(slice_sql.contains("_window_id > $cutoff"));
    assert!(slice_sql.contains("GROUP BY book"));
}

#[test]
fn test_decomposability_detection() {
    use hydrocube::aggregation::decompose::is_decomposable;
    assert!(is_decomposable("SUM(notional)"));
    assert!(is_decomposable("COUNT(*)"));
    assert!(is_decomposable("MAX(trade_time)"));
    assert!(is_decomposable("MIN(price)"));
    assert!(is_decomposable("AVG(price)"));
    assert!(!is_decomposable("COUNT(DISTINCT counterparty)"));
    assert!(!is_decomposable("MEDIAN(price)"));
    assert!(!is_decomposable("QUANTILE_CONT(price, 0.95)"));
}

#[test]
fn test_full_aggregation_sql_is_unchanged() {
    let sql = "SELECT book, SUM(notional) AS total FROM slices GROUP BY book";
    let gen = AggSqlGenerator::from_user_sql(sql).unwrap();
    assert_eq!(gen.full_aggregation_sql(), sql);
}

#[test]
fn test_has_non_decomposable_false_when_all_decomposable() {
    let sql = "SELECT book, SUM(notional) AS total, COUNT(*) AS cnt FROM slices GROUP BY book";
    let gen = AggSqlGenerator::from_user_sql(sql).unwrap();
    assert!(!gen.has_non_decomposable());
}

#[test]
fn test_has_non_decomposable_true_when_present() {
    let sql = "SELECT book, COUNT(DISTINCT counterparty) AS uniq FROM slices GROUP BY book";
    let gen = AggSqlGenerator::from_user_sql(sql).unwrap();
    assert!(gen.has_non_decomposable());
}

#[test]
fn test_multiline_sql_from_example_yaml() {
    // Mirrors the cube.example.yaml aggregation SQL (multi-line, CASE WHEN, etc.)
    let sql = r#"SELECT
      book,
      desk,
      instrument_type,
      currency,
      SUM(notional) AS total_notional,
      SUM(CASE WHEN side = 'BUY' THEN quantity ELSE -quantity END) AS net_quantity,
      COUNT(*) AS trade_count,
      AVG(price) AS avg_price,
      MAX(trade_time) AS max_trade_time
    FROM slices
    GROUP BY book, desk, instrument_type, currency"#;

    let gen = AggSqlGenerator::from_user_sql(sql).unwrap();
    assert_eq!(
        gen.dimensions(),
        &["book", "desk", "instrument_type", "currency"]
    );
    assert_eq!(gen.measures().len(), 5);

    let aliases: Vec<&str> = gen.measures().iter().map(|m| m.alias.as_str()).collect();
    assert!(aliases.contains(&"total_notional"));
    assert!(aliases.contains(&"net_quantity"));
    assert!(aliases.contains(&"trade_count"));
    assert!(aliases.contains(&"avg_price"));
    assert!(aliases.contains(&"max_trade_time"));

    // All measures in this SQL are decomposable
    assert!(!gen.has_non_decomposable());

    let slice_sql = gen.slice_aggregation_sql();
    assert!(slice_sql.contains("_window_id > $cutoff"));
    assert!(slice_sql.contains("GROUP BY book, desk, instrument_type, currency"));
}

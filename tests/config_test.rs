use hydrocube::config::{
    AggregationConfig, CubeConfig, DataFormat, LoadTiming, PublishHookConfig,
    SourceConfig, SourceType, TableConfig, TableMode,
};

fn minimal_new_config_yaml() -> &'static str {
    r#"
name: test_cube
tables:
  - name: trades
    mode: append
    schema:
      columns:
        - { name: book, type: VARCHAR }
        - { name: notional, type: DOUBLE }
sources:
  - type: kafka
    table: trades
    format: json
    brokers: ["localhost:9092"]
    topic: trades.executed
    group_id: test-group
window:
  interval_ms: 1000
persistence:
  enabled: false
  path: ":memory:"
  flush_interval: 10
aggregation:
  key_columns: [book]
  publish:
    sql: "SELECT book, SUM(notional) AS total FROM {trades} GROUP BY book"
"#
}

#[test]
fn test_new_config_parses() {
    let cfg: CubeConfig = serde_yaml::from_str(minimal_new_config_yaml()).unwrap();
    assert_eq!(cfg.name, "test_cube");
    assert_eq!(cfg.tables.len(), 1);
    assert_eq!(cfg.tables[0].name, "trades");
    assert!(matches!(cfg.tables[0].mode, TableMode::Append));
    assert_eq!(cfg.sources.len(), 1);
    assert!(matches!(cfg.sources[0].source_type, SourceType::Kafka));
    assert_eq!(cfg.aggregation.key_columns, vec!["book"]);
    assert!(cfg.aggregation.startup.is_none());
    assert!(cfg.aggregation.compaction.is_none());
}

#[test]
fn test_table_modes_parse() {
    let yaml = r#"
name: t
tables:
  - { name: a, mode: append,    schema: { columns: [{name: x, type: VARCHAR}] } }
  - { name: b, mode: replace,   key_columns: [x], schema: { columns: [{name: x, type: VARCHAR}] } }
  - { name: c, mode: reference, schema: { columns: [{name: x, type: VARCHAR}] } }
sources: []
window: { interval_ms: 1000 }
persistence: { enabled: false, path: ":memory:", flush_interval: 10 }
aggregation:
  key_columns: [x]
  publish:
    sql: "SELECT x FROM {a} GROUP BY x"
"#;
    let cfg: CubeConfig = serde_yaml::from_str(yaml).unwrap();
    assert!(matches!(cfg.tables[0].mode, TableMode::Append));
    assert!(matches!(cfg.tables[1].mode, TableMode::Replace));
    assert!(matches!(cfg.tables[2].mode, TableMode::Reference));
}

#[test]
fn test_source_load_timing_parses() {
    let yaml = r#"
name: t
tables:
  - { name: data, mode: append, schema: { columns: [{name: x, type: VARCHAR}] } }
sources:
  - name: sod_file
    type: file
    table: data
    format: parquet
    load: reset
    path: /data/file.parquet
window: { interval_ms: 1000 }
persistence: { enabled: false, path: ":memory:", flush_interval: 10 }
aggregation:
  key_columns: [x]
  publish:
    sql: "SELECT x FROM {data} GROUP BY x"
"#;
    let cfg: CubeConfig = serde_yaml::from_str(yaml).unwrap();
    assert!(matches!(cfg.sources[0].load, Some(LoadTiming::Reset)));
    assert_eq!(cfg.sources[0].name.as_deref(), Some("sod_file"));
}

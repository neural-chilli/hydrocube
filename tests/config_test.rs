use hydrocube::config::{CubeConfig, LoadTiming, SourceType, TableMode};

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

fn make_valid_config() -> CubeConfig {
    serde_yaml::from_str(minimal_new_config_yaml()).unwrap()
}

#[test]
fn test_validate_rejects_empty_key_columns() {
    let yaml = minimal_new_config_yaml().replace("key_columns: [book]", "key_columns: []");
    let cfg: CubeConfig = serde_yaml::from_str(&yaml).unwrap();
    let err = cfg.validate().unwrap_err().to_string();
    assert!(err.contains("key_columns"), "got: {err}");
}

#[test]
fn test_validate_rejects_bad_window() {
    let yaml = minimal_new_config_yaml().replace("interval_ms: 1000", "interval_ms: 50");
    let cfg: CubeConfig = serde_yaml::from_str(&yaml).unwrap();
    let err = cfg.validate().unwrap_err().to_string();
    assert!(err.contains("interval_ms"), "got: {err}");
}

#[test]
fn test_validate_replace_table_without_key_columns_fails() {
    let yaml = r#"
name: t
tables:
  - name: md
    mode: replace
    schema:
      columns:
        - { name: curve, type: VARCHAR }
sources: []
window: { interval_ms: 1000 }
persistence: { enabled: false, path: ":memory:", flush_interval: 10 }
aggregation:
  key_columns: [curve]
  publish:
    sql: "SELECT curve FROM {md}"
"#;
    let cfg: CubeConfig = serde_yaml::from_str(yaml).unwrap();
    let err = cfg.validate().unwrap_err().to_string();
    assert!(err.contains("key_columns"), "got: {err}");
}

#[test]
fn test_validate_duplicate_table_names_fails() {
    let yaml = r#"
name: t
tables:
  - { name: trades, mode: append, schema: { columns: [{name: x, type: VARCHAR}] } }
  - { name: trades, mode: append, schema: { columns: [{name: x, type: VARCHAR}] } }
sources: []
window: { interval_ms: 1000 }
persistence: { enabled: false, path: ":memory:", flush_interval: 10 }
aggregation:
  key_columns: [x]
  publish:
    sql: "SELECT x FROM {trades}"
"#;
    let cfg: CubeConfig = serde_yaml::from_str(yaml).unwrap();
    let err = cfg.validate().unwrap_err().to_string();
    assert!(
        err.contains("duplicate") || err.contains("trades"),
        "got: {err}"
    );
}

#[test]
fn test_validate_invalid_compaction_interval_fails() {
    let yaml = r#"
name: t
tables:
  - { name: ev, mode: append, schema: { columns: [{name: x, type: VARCHAR}] } }
sources: []
window: { interval_ms: 1000 }
persistence: { enabled: false, path: ":memory:", flush_interval: 10 }
aggregation:
  key_columns: [x]
  compaction:
    interval: "5x"
  publish:
    sql: "SELECT x FROM {ev}"
"#;
    let cfg: CubeConfig = serde_yaml::from_str(yaml).unwrap();
    let err = cfg.validate().unwrap_err().to_string();
    assert!(err.contains("interval") || err.contains("5x"), "got: {err}");
}

#[test]
fn test_validate_valid_config_passes() {
    let cfg = make_valid_config();
    cfg.validate().expect("valid config should pass validate()");
}

#[test]
fn test_parse_duration_str() {
    use hydrocube::config::parse_duration_str;
    assert_eq!(parse_duration_str("7d").unwrap(), 7 * 86_400);
    assert_eq!(parse_duration_str("24h").unwrap(), 24 * 3_600);
    assert_eq!(parse_duration_str("90m").unwrap(), 90 * 60);
    assert_eq!(parse_duration_str("120s").unwrap(), 120);
    assert!(parse_duration_str("bad").is_err());
    assert!(parse_duration_str("5x").is_err());
    assert!(parse_duration_str("").is_err());
}

#[test]
fn test_schema_hash_changes_when_publish_sql_changes() {
    let cfg1: CubeConfig = serde_yaml::from_str(minimal_new_config_yaml()).unwrap();
    let yaml2 = minimal_new_config_yaml().replace(
        "SELECT book, SUM(notional) AS total FROM {trades} GROUP BY book",
        "SELECT book, COUNT(*) AS n FROM {trades} GROUP BY book",
    );
    let cfg2: CubeConfig = serde_yaml::from_str(&yaml2).unwrap();
    assert_ne!(cfg1.schema_hash(), cfg2.schema_hash());
}

#[test]
fn test_schema_hash_changes_when_table_schema_changes() {
    let cfg1: CubeConfig = serde_yaml::from_str(minimal_new_config_yaml()).unwrap();
    let yaml2 = minimal_new_config_yaml().replace(
        "- { name: notional, type: DOUBLE }",
        "- { name: notional, type: DOUBLE }\n        - { name: currency, type: VARCHAR }",
    );
    let cfg2: CubeConfig = serde_yaml::from_str(&yaml2).unwrap();
    assert_ne!(cfg1.schema_hash(), cfg2.schema_hash());
}

#[test]
fn test_schema_hash_is_deterministic() {
    let cfg: CubeConfig = serde_yaml::from_str(minimal_new_config_yaml()).unwrap();
    assert_eq!(cfg.schema_hash(), cfg.schema_hash());
}

#[test]
fn test_schema_hash_stable_when_window_changes() {
    // Window interval is not a schema-affecting change — no --rebuild required
    let cfg1: CubeConfig = serde_yaml::from_str(minimal_new_config_yaml()).unwrap();
    let yaml2 = minimal_new_config_yaml().replace("interval_ms: 1000", "interval_ms: 500");
    let cfg2: CubeConfig = serde_yaml::from_str(&yaml2).unwrap();
    assert_eq!(cfg1.schema_hash(), cfg2.schema_hash());
}

#[test]
fn test_source_config_parses_avro_schema() {
    use hydrocube::config::{CubeConfig, DataFormat};
    let yaml = r#"
name: test
tables:
  - name: trades
    mode: append
    schema:
      columns:
        - { name: trade_id, type: VARCHAR }
sources:
  - type: kafka
    topic: trades
    table: trades
    format: avro
    avro_schema: '{"type":"record","name":"Trade","fields":[{"name":"trade_id","type":"string"}]}'
window: { interval_ms: 1000 }
persistence: { path: ":memory:", flush_interval: 10 }
aggregation:
  key_columns: [trade_id]
  publish:
    sql: "SELECT trade_id FROM trades"
"#;
    let config: CubeConfig = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(config.sources[0].format, DataFormat::Avro);
    assert!(config.sources[0].avro_schema.is_some());
}

#[test]
fn test_source_config_parses_protobuf_fields() {
    use hydrocube::config::{CubeConfig, DataFormat};
    let yaml = r#"
name: test
tables:
  - name: rates
    mode: replace
    key_columns: [curve]
    schema:
      columns:
        - { name: curve, type: VARCHAR }
sources:
  - type: kafka
    topic: rates
    table: rates
    format: protobuf
    proto_schema: |
      syntax = "proto3";
      message RateUpdate { string curve = 1; }
    proto_message: RateUpdate
window: { interval_ms: 1000 }
persistence: { path: ":memory:", flush_interval: 10 }
aggregation:
  key_columns: [curve]
  publish:
    sql: "SELECT curve FROM rates"
"#;
    let config: CubeConfig = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(config.sources[0].format, DataFormat::Protobuf);
    assert!(config.sources[0].proto_schema.is_some());
    assert_eq!(
        config.sources[0].proto_message.as_deref(),
        Some("RateUpdate")
    );
}

#[test]
fn test_refresh_strategy_startup_parses() {
    use hydrocube::config::CubeConfig;
    let yaml = r#"
name: test
tables:
  - name: instruments
    mode: reference
    schema:
      columns:
        - { name: instrument_id, type: VARCHAR }
sources:
  - type: file
    path: /data/instruments.csv
    table: instruments
    format: csv
    refresh: startup
window: { interval_ms: 1000 }
persistence: { path: ":memory:", flush_interval: 10 }
aggregation:
  key_columns: [instrument_id]
  publish:
    sql: "SELECT instrument_id FROM instruments"
"#;
    let config: CubeConfig = serde_yaml::from_str(yaml).unwrap();
    assert!(config.sources[0]
        .refresh
        .as_ref()
        .map(|r| r.is_startup())
        .unwrap_or(false));
}

#[test]
fn test_refresh_strategy_interval_parses() {
    use hydrocube::config::CubeConfig;
    let yaml = r#"
name: test
tables:
  - name: instruments
    mode: reference
    schema:
      columns:
        - { name: instrument_id, type: VARCHAR }
sources:
  - type: file
    path: /data/instruments.csv
    table: instruments
    format: csv
    refresh:
      interval: 1h
window: { interval_ms: 1000 }
persistence: { path: ":memory:", flush_interval: 10 }
aggregation:
  key_columns: [instrument_id]
  publish:
    sql: "SELECT instrument_id FROM instruments"
"#;
    let config: CubeConfig = serde_yaml::from_str(yaml).unwrap();
    assert!(config.sources[0]
        .refresh
        .as_ref()
        .map(|r| r.interval_str().is_some())
        .unwrap_or(false));
}

#[test]
fn test_validate_rejects_avro_source_without_schema() {
    use hydrocube::config::CubeConfig;
    let yaml = r#"
name: test
tables:
  - name: trades
    mode: append
    schema:
      columns:
        - { name: trade_id, type: VARCHAR }
sources:
  - type: kafka
    topic: trades
    table: trades
    format: avro
window: { interval_ms: 1000 }
persistence: { path: ":memory:", flush_interval: 10 }
aggregation:
  key_columns: [trade_id]
  publish:
    sql: "SELECT trade_id FROM trades"
"#;
    let config: CubeConfig = serde_yaml::from_str(yaml).unwrap();
    let result = config.validate();
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("avro_schema"),
        "expected avro_schema error, got: {msg}"
    );
}

#[test]
fn test_validate_rejects_protobuf_source_without_proto_message() {
    use hydrocube::config::CubeConfig;
    let yaml = r#"
name: test
tables:
  - name: rates
    mode: replace
    key_columns: [curve]
    schema:
      columns:
        - { name: curve, type: VARCHAR }
sources:
  - type: kafka
    topic: rates
    table: rates
    format: protobuf
    proto_schema: |
      syntax = "proto3";
      message RateUpdate { string curve = 1; }
window: { interval_ms: 1000 }
persistence: { path: ":memory:", flush_interval: 10 }
aggregation:
  key_columns: [curve]
  publish:
    sql: "SELECT curve FROM rates"
"#;
    let config: CubeConfig = serde_yaml::from_str(yaml).unwrap();
    let result = config.validate();
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("proto_message"),
        "expected proto_message error, got: {msg}"
    );
}

#[test]
fn test_validate_rejects_replace_table_key_column_not_in_schema() {
    use hydrocube::config::CubeConfig;
    let yaml = r#"
name: test
tables:
  - name: market_data
    mode: replace
    key_columns: [curve, tenor]
    schema:
      columns:
        - { name: curve, type: VARCHAR }
        - { name: rate, type: DOUBLE }
sources: []
window: { interval_ms: 1000 }
persistence: { path: ":memory:", flush_interval: 10 }
aggregation:
  key_columns: [curve]
  publish:
    sql: "SELECT curve FROM market_data"
"#;
    let config: CubeConfig = serde_yaml::from_str(yaml).unwrap();
    let result = config.validate();
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("tenor"),
        "error should mention missing key column 'tenor', got: {msg}"
    );
}

#[test]
fn test_validate_accepts_replace_table_all_key_columns_in_schema() {
    use hydrocube::config::CubeConfig;
    let yaml = r#"
name: test
tables:
  - name: market_data
    mode: replace
    key_columns: [curve, tenor]
    schema:
      columns:
        - { name: curve, type: VARCHAR }
        - { name: tenor, type: VARCHAR }
        - { name: rate, type: DOUBLE }
sources: []
window: { interval_ms: 1000 }
persistence: { path: ":memory:", flush_interval: 10 }
aggregation:
  key_columns: [curve]
  publish:
    sql: "SELECT curve FROM market_data"
"#;
    let config: CubeConfig = serde_yaml::from_str(yaml).unwrap();
    assert!(config.validate().is_ok());
}

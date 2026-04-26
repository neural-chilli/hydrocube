// tests/nats_test.rs
use hydrocube::config::{CubeConfig, NatsMode, SourceType};

#[test]
fn test_nats_core_source_parses() {
    let yaml = r#"
name: t
tables:
  - name: md
    mode: replace
    key_columns: [curve]
    schema:
      columns:
        - { name: curve, type: VARCHAR }
        - { name: rate, type: DOUBLE }
sources:
  - type: nats
    table: md
    format: json
    url: nats://localhost:4222
    subject: rates.live
    mode: core
window: { interval_ms: 1000 }
persistence: { enabled: false, path: ":memory:", flush_interval: 10 }
aggregation:
  key_columns: [curve]
  publish:
    sql: "SELECT curve, AVG(rate) AS rate FROM md GROUP BY curve"
"#;
    let cfg: CubeConfig = serde_yaml::from_str(yaml).unwrap();
    assert!(matches!(cfg.sources[0].source_type, SourceType::Nats));
    assert!(matches!(cfg.sources[0].mode, Some(NatsMode::Core)));
}

#[test]
fn test_nats_jetstream_source_parses() {
    let yaml = r#"
name: t
tables:
  - name: md
    mode: replace
    key_columns: [curve]
    schema:
      columns:
        - { name: curve, type: VARCHAR }
        - { name: rate, type: DOUBLE }
sources:
  - type: nats
    table: md
    format: json
    url: nats://localhost:4222
    subject: rates.live
    mode: jetstream
    stream: RATES
    consumer: hydrocube-md
window: { interval_ms: 1000 }
persistence: { enabled: false, path: ":memory:", flush_interval: 10 }
aggregation:
  key_columns: [curve]
  publish:
    sql: "SELECT curve, AVG(rate) AS rate FROM md GROUP BY curve"
"#;
    let cfg: CubeConfig = serde_yaml::from_str(yaml).unwrap();
    assert!(matches!(cfg.sources[0].source_type, SourceType::Nats));
    assert!(matches!(cfg.sources[0].mode, Some(NatsMode::Jetstream)));
    assert_eq!(cfg.sources[0].stream.as_deref(), Some("RATES"));
    assert_eq!(cfg.sources[0].consumer.as_deref(), Some("hydrocube-md"));
}

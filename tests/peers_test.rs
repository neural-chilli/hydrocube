use hydrocube::config::{CubeConfig, PeersConfig};

#[test]
fn peers_config_deserializes_with_defaults() {
    let yaml = r#"
url: "http://localhost:8080"
description: "Test cube"
seeds:
  - "http://cube2.internal"
"#;
    let peers: PeersConfig = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(peers.url, "http://localhost:8080");
    assert_eq!(peers.description, "Test cube");
    assert_eq!(peers.seeds, vec!["http://cube2.internal"]);
    assert_eq!(peers.health_check_interval_secs, 30);
    assert_eq!(peers.health_check_failures_before_offline, 3);
}

#[test]
fn cube_config_peers_defaults_to_none() {
    let yaml = r#"
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
"#;
    let config: CubeConfig = serde_yaml::from_str(yaml).unwrap();
    assert!(config.peers.is_none());
}

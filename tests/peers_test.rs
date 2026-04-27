use hydrocube::config::{CubeConfig, PeersConfig};
use hydrocube::peers::{PeerRecord, PeerRegistry, PeerStatus};

fn make_record(name: &str, url: &str) -> PeerRecord {
    PeerRecord {
        name: name.into(),
        url: url.into(),
        description: format!("{} desc", name),
        status: PeerStatus::Online,
    }
}

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

#[test]
fn registry_upsert_and_list() {
    let registry = PeerRegistry::new(make_record("self", "http://localhost:8080"));
    registry.upsert(make_record("peer_a", "http://cube-a:8080"));
    registry.upsert(make_record("peer_b", "http://cube-b:8080"));

    let peers = registry.list();
    assert_eq!(peers.len(), 2);
    assert!(peers.iter().any(|p| p.url == "http://cube-a:8080"));
    assert!(peers.iter().any(|p| p.url == "http://cube-b:8080"));
}

#[test]
fn registry_upsert_is_idempotent() {
    let registry = PeerRegistry::new(make_record("self", "http://localhost:8080"));
    let peer = make_record("peer_a", "http://cube-a:8080");
    registry.upsert(peer.clone());
    registry.upsert(peer.clone());
    assert_eq!(registry.list().len(), 1);
}

#[test]
fn registry_upsert_ignores_own_url() {
    let registry = PeerRegistry::new(make_record("self", "http://localhost:8080"));
    registry.upsert(make_record("self_alias", "http://localhost:8080"));
    assert_eq!(registry.list().len(), 0);
}

#[test]
fn registry_record_failure_marks_offline_after_threshold() {
    let registry = PeerRegistry::new(make_record("self", "http://localhost:8080"));
    registry.upsert(make_record("peer_a", "http://cube-a:8080"));

    // Two failures — still online (threshold is 3).
    registry.record_failure("http://cube-a:8080", 3);
    registry.record_failure("http://cube-a:8080", 3);
    assert_eq!(registry.list()[0].status, PeerStatus::Online);

    // Third failure — now offline.
    registry.record_failure("http://cube-a:8080", 3);
    assert_eq!(registry.list()[0].status, PeerStatus::Offline);
}

#[test]
fn registry_record_success_resets_to_online() {
    let registry = PeerRegistry::new(make_record("self", "http://localhost:8080"));
    registry.upsert(make_record("peer_a", "http://cube-a:8080"));

    registry.record_failure("http://cube-a:8080", 1);
    assert_eq!(registry.list()[0].status, PeerStatus::Offline);

    registry.record_success("http://cube-a:8080");
    assert_eq!(registry.list()[0].status, PeerStatus::Online);
}

#[test]
fn registry_peer_urls_except_excludes_given_url() {
    let registry = PeerRegistry::new(make_record("self", "http://localhost:8080"));
    registry.upsert(make_record("a", "http://cube-a:8080"));
    registry.upsert(make_record("b", "http://cube-b:8080"));
    registry.upsert(make_record("c", "http://cube-c:8080"));

    let urls = registry.peer_urls_except("http://cube-b:8080");
    assert_eq!(urls.len(), 2);
    assert!(!urls.contains(&"http://cube-b:8080".to_string()));
}

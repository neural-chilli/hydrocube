# Multi-Cube Peer Discovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add gossip-style peer discovery so multiple HydroCube processes can find each other and be navigated between from a single UI dropdown.

**Architecture:** Each cube maintains an in-memory `PeerRegistry`. On startup it bootstraps from configured seed URLs (GET /api/peers, then POST /api/peers/register); seeds forward registrations one hop to their known peers. A background health-check loop marks unreachable peers offline. The UI fetches `/api/peers` on load and populates a dynamic cube selector; clicking a peer does a full URL-swap navigation.

**Tech Stack:** Rust/Axum (new `src/peers/` module), `reqwest` 0.12 for outbound HTTP, Minijinja template (dynamic cube selector + not-authorised overlay), no new infrastructure required.

---

## File Map

| Action   | Path                                           | Responsibility                                              |
|----------|------------------------------------------------|-------------------------------------------------------------|
| Modify   | `Cargo.toml`                                   | Add `reqwest` dependency                                    |
| Modify   | `src/config.rs`                                | Add `PeersConfig` struct + `peers` field on `CubeConfig`   |
| Modify   | `src/lib.rs`                                   | Expose `pub mod peers`                                      |
| **Create** | `src/peers/mod.rs`                           | `PeerRecord`, `PeerStatus`, `PeerRegistry`                  |
| **Create** | `src/peers/gossip.rs`                        | `bootstrap()` + `run_health_checks()`                       |
| Modify   | `src/web/api.rs`                               | Add `peer_registry`/`http_client` to `AppState`; two new handlers |
| Modify   | `src/web/server.rs`                            | Wire new routes; accept `peer_registry` param               |
| Modify   | `src/main.rs`                                  | Build registry, spawn gossip + health-check tasks           |
| Modify   | `templates/index.html.j2`                      | Dynamic cube selector + not-authorised overlay              |
| Modify   | `tests/integration_test.rs` (+ 4 others)       | Add `peers: None` to `CubeConfig` literals                  |
| **Create** | `tests/peers_test.rs`                        | Unit tests for `PeerRegistry` + gossip logic                |

---

### Task 1: Add reqwest to Cargo.toml

**Files:**
- Modify: `Cargo.toml`

- [ ] **Step 1: Add the dependency**

Open `Cargo.toml` and add after the `tokio-stream` line in `[dependencies]`:

```toml
reqwest = { version = "0.12", default-features = false, features = ["json", "rustls-tls"] }
```

- [ ] **Step 2: Verify it compiles**

```bash
cargo build --no-default-features 2>&1 | tail -5
```

Expected: build succeeds (pre-existing warnings only — no new errors).

- [ ] **Step 3: Commit**

```bash
git add Cargo.toml
git commit -m "chore: add reqwest for outbound peer HTTP calls"
```

---

### Task 2: Add PeersConfig to config.rs

**Files:**
- Modify: `src/config.rs`
- Create: `tests/peers_test.rs`

- [ ] **Step 1: Write the failing tests**

Create `tests/peers_test.rs`:

```rust
// tests/peers_test.rs

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
name: test
source:
  type: test
schema:
  columns:
    - name: id
      type: VARCHAR
aggregation:
  sql: "SELECT id FROM slices GROUP BY id"
window:
  interval_ms: 1000
compaction:
  interval_windows: 60
retention:
  duration: 1d
  parquet_path: /tmp/test
persistence:
  enabled: false
  path: ":memory:"
  flush_interval: 10
"#;
    let config: CubeConfig = serde_yaml::from_str(yaml).unwrap();
    assert!(config.peers.is_none());
}
```

- [ ] **Step 2: Run test to confirm it fails**

```bash
cargo test --test peers_test --no-default-features 2>&1 | tail -10
```

Expected: compile error — `PeersConfig` not found.

- [ ] **Step 3: Add PeersConfig and update CubeConfig**

In `src/config.rs`, add after the `AuthConfig` block:

```rust
#[derive(Debug, Deserialize, Clone)]
pub struct PeersConfig {
    /// This cube's externally reachable base URL (e.g. "https://cube1.internal").
    pub url: String,
    /// Short label shown in other cubes' selectors.
    pub description: String,
    /// Seed cube URLs to bootstrap from on startup.
    #[serde(default)]
    pub seeds: Vec<String>,
    /// How often (seconds) to ping each peer's /api/status. Default: 30.
    #[serde(default = "default_health_check_interval")]
    pub health_check_interval_secs: u64,
    /// Consecutive health-check failures before marking a peer offline. Default: 3.
    #[serde(default = "default_health_check_failures")]
    pub health_check_failures_before_offline: u32,
}

fn default_health_check_interval() -> u64 {
    30
}

fn default_health_check_failures() -> u32 {
    3
}
```

Add to the `CubeConfig` struct, after the `auth` field:

```rust
    #[serde(default)]
    pub peers: Option<PeersConfig>,
```

- [ ] **Step 4: Run test to confirm it passes**

```bash
cargo test --test peers_test --no-default-features 2>&1 | tail -10
```

Expected: both `peers_test` tests pass.

- [ ] **Step 5: Fix CubeConfig struct literals in existing test files**

Five test files construct `CubeConfig` by field — they will no longer compile. Add `peers: None,` after `auth: None,` in each:

Files to update: `tests/aggregation_test.rs`, `tests/compaction_test.rs`, `tests/engine_test.rs`, `tests/integration_test.rs`, `tests/persistence_test.rs`.

```bash
grep -n "auth: None" tests/aggregation_test.rs tests/compaction_test.rs tests/engine_test.rs tests/integration_test.rs tests/persistence_test.rs
```

For each matching location add `peers: None,` on the line immediately after `auth: None,`.

- [ ] **Step 6: Confirm all existing tests still pass**

```bash
cargo test --no-default-features 2>&1 | tail -15
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/config.rs tests/peers_test.rs tests/aggregation_test.rs tests/compaction_test.rs tests/engine_test.rs tests/integration_test.rs tests/persistence_test.rs
git commit -m "feat: add PeersConfig to CubeConfig"
```

---

### Task 3: Create src/peers/mod.rs — PeerRegistry

**Files:**
- Create: `src/peers/mod.rs`
- Create: `src/peers/gossip.rs` (stub)
- Modify: `src/lib.rs`

- [ ] **Step 1: Write failing tests**

Append to `tests/peers_test.rs`:

```rust
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
```

- [ ] **Step 2: Run test to confirm it fails**

```bash
cargo test --test peers_test --no-default-features 2>&1 | tail -10
```

Expected: compile error — `hydrocube::peers` not found.

- [ ] **Step 3: Create src/peers/mod.rs**

```rust
// src/peers/mod.rs
//
// In-memory peer registry — thread-safe store of known HydroCube peers.

pub mod gossip;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum PeerStatus {
    Online,
    Offline,
}

/// Describes a single HydroCube peer as seen by the registry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerRecord {
    pub name: String,
    /// Base URL of the peer's HTTP server (e.g. "https://cube1.internal").
    /// Used for health checks and gossip forwarding; not displayed in the UI.
    pub url: String,
    pub description: String,
    pub status: PeerStatus,
}

// ---------------------------------------------------------------------------
// PeerRegistry
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct PeerRegistry {
    inner: Arc<Mutex<RegistryInner>>,
}

struct RegistryInner {
    own_info: PeerRecord,
    peers: HashMap<String, PeerEntry>,
}

struct PeerEntry {
    record: PeerRecord,
    consecutive_failures: u32,
}

impl PeerRegistry {
    /// Create a new registry. `own_info` describes this cube.
    pub fn new(own_info: PeerRecord) -> Self {
        Self {
            inner: Arc::new(Mutex::new(RegistryInner {
                own_info,
                peers: HashMap::new(),
            })),
        }
    }

    /// Return this cube's own info record.
    pub fn own_info(&self) -> PeerRecord {
        self.inner.lock().unwrap().own_info.clone()
    }

    /// Insert or update a peer entry (always resets to Online and clears failure count).
    /// Silently ignores entries whose URL matches own_info.url.
    pub fn upsert(&self, record: PeerRecord) {
        let mut inner = self.inner.lock().unwrap();
        if record.url == inner.own_info.url {
            return;
        }
        inner.peers.insert(
            record.url.clone(),
            PeerEntry {
                record: PeerRecord {
                    status: PeerStatus::Online,
                    ..record
                },
                consecutive_failures: 0,
            },
        );
    }

    /// Return all tracked peers (does NOT include this cube's own_info).
    pub fn list(&self) -> Vec<PeerRecord> {
        self.inner
            .lock()
            .unwrap()
            .peers
            .values()
            .map(|e| e.record.clone())
            .collect()
    }

    /// Record one health-check failure. Marks the peer offline once
    /// `max_failures` consecutive failures accumulate.
    pub fn record_failure(&self, url: &str, max_failures: u32) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(entry) = inner.peers.get_mut(url) {
            entry.consecutive_failures += 1;
            if entry.consecutive_failures >= max_failures {
                entry.record.status = PeerStatus::Offline;
            }
        }
    }

    /// Record a successful health-check: reset failure counter and mark online.
    pub fn record_success(&self, url: &str) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(entry) = inner.peers.get_mut(url) {
            entry.consecutive_failures = 0;
            entry.record.status = PeerStatus::Online;
        }
    }

    /// Return peer URLs for one-hop forwarding: all tracked peers except `exclude_url`.
    pub fn peer_urls_except(&self, exclude_url: &str) -> Vec<String> {
        self.inner
            .lock()
            .unwrap()
            .peers
            .keys()
            .filter(|u| u.as_str() != exclude_url)
            .cloned()
            .collect()
    }
}
```

- [ ] **Step 4: Create src/peers/gossip.rs stub**

```rust
// src/peers/gossip.rs — implemented in Task 4
```

- [ ] **Step 5: Expose peers module in src/lib.rs**

Add after `pub mod persistence;`:

```rust
pub mod peers;
```

- [ ] **Step 6: Run tests to confirm they pass**

```bash
cargo test --test peers_test --no-default-features 2>&1 | tail -20
```

Expected: all eight `peers_test` tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/peers/mod.rs src/peers/gossip.rs src/lib.rs tests/peers_test.rs
git commit -m "feat: add PeerRegistry with upsert, failure tracking, and online/offline state"
```

---

### Task 4: Implement src/peers/gossip.rs

**Files:**
- Modify: `src/peers/gossip.rs`

Bootstrap sequence:
1. `GET /api/peers` from each seed → merge their known peers into our registry.
2. `POST /api/peers/register` to each seed → seed forwards us to its peers and responds with its full peer list (one-round-trip convergence).

The `forwarded: bool` field on registration requests breaks gossip loops: seeds forward with `forwarded: true`; recipients of a forwarded registration do not forward further.

- [ ] **Step 1: Write the full implementation**

Replace `src/peers/gossip.rs` with:

```rust
// src/peers/gossip.rs
//
// Gossip bootstrap and background health-check loop.
//
// Bootstrap sequence (runs once at startup):
//   1. GET /api/peers from each seed — learn existing fleet members.
//   2. POST /api/peers/register to each seed — announce ourselves.
//      Seed forwards the registration to its other peers (one hop, forwarded=true).
//      Seed responds with its current peer list for one-round-trip convergence.
//
// Health checks (run forever):
//   Background task pings /api/status on every known peer every N seconds.
//   Consecutive failures above the configured threshold mark a peer offline.
//   Recovery resets the counter and restores online status automatically.

use std::time::Duration;

use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::config::PeersConfig;
use crate::peers::{PeerRecord, PeerRegistry, PeerStatus};

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct GetPeersResponse {
    #[serde(rename = "self")]
    self_info: PeerRecord,
    peers: Vec<PeerRecord>,
}

#[derive(Serialize)]
struct RegisterRequest<'a> {
    name: &'a str,
    url: &'a str,
    description: &'a str,
    /// Set true when forwarding so the recipient does not forward again.
    forwarded: bool,
}

// ---------------------------------------------------------------------------
// Bootstrap
// ---------------------------------------------------------------------------

/// Perform startup gossip bootstrap against all configured seeds.
/// Seeds that cannot be reached are logged as warnings; the cube starts
/// standalone and gains peers as seeds come back up during health checks.
pub async fn bootstrap(
    own_info: &PeerRecord,
    config: &PeersConfig,
    registry: &PeerRegistry,
    client: &Client,
) {
    for seed_url in &config.seeds {
        let base = seed_url.trim_end_matches('/');

        // Step 1: fetch known peers from seed.
        let peers_url = format!("{base}/api/peers");
        match client
            .get(&peers_url)
            .timeout(Duration::from_secs(10))
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                match resp.json::<GetPeersResponse>().await {
                    Ok(body) => {
                        registry.upsert(body.self_info);
                        for peer in body.peers {
                            if peer.url != own_info.url {
                                registry.upsert(peer);
                            }
                        }
                        info!(target: "hydrocube::peers", "Fetched peer list from seed {}", seed_url);
                    }
                    Err(e) => {
                        warn!(target: "hydrocube::peers", "Could not parse peers response from {}: {}", seed_url, e);
                    }
                }
            }
            Ok(resp) => {
                warn!(target: "hydrocube::peers", "Seed {} returned HTTP {} for GET /api/peers", seed_url, resp.status());
            }
            Err(e) => {
                warn!(target: "hydrocube::peers", "Could not reach seed {} for GET /api/peers: {}", seed_url, e);
            }
        }

        // Step 2: register ourselves with seed.
        let register_url = format!("{base}/api/peers/register");
        let body = RegisterRequest {
            name: &own_info.name,
            url: &own_info.url,
            description: &own_info.description,
            forwarded: false,
        };
        match client
            .post(&register_url)
            .json(&body)
            .timeout(Duration::from_secs(10))
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                // Seed returns its current peer list — merge for one-round-trip convergence.
                match resp.json::<Vec<PeerRecord>>().await {
                    Ok(peers) => {
                        for peer in peers {
                            if peer.url != own_info.url {
                                registry.upsert(peer);
                            }
                        }
                    }
                    Err(e) => {
                        warn!(target: "hydrocube::peers", "Could not parse register response from {}: {}", seed_url, e);
                    }
                }
                info!(target: "hydrocube::peers", "Registered with seed {}", seed_url);
            }
            Ok(resp) => {
                warn!(target: "hydrocube::peers", "Seed {} returned HTTP {} for POST /api/peers/register", seed_url, resp.status());
            }
            Err(e) => {
                warn!(target: "hydrocube::peers", "Registration to {} failed: {}", seed_url, e);
            }
        }
    }

    info!(
        target: "hydrocube::peers",
        "Bootstrap complete — {} peer(s) known",
        registry.list().len()
    );
}

// ---------------------------------------------------------------------------
// Health checks
// ---------------------------------------------------------------------------

/// Background task: ping every known peer's /api/status on a fixed interval.
/// Runs indefinitely; cancel by dropping the task handle.
pub async fn run_health_checks(registry: PeerRegistry, config: PeersConfig, client: Client) {
    let interval = Duration::from_secs(config.health_check_interval_secs);
    let max_failures = config.health_check_failures_before_offline;

    loop {
        tokio::time::sleep(interval).await;

        for peer in registry.list() {
            let status_url = format!("{}/api/status", peer.url.trim_end_matches('/'));
            match client
                .get(&status_url)
                .timeout(Duration::from_secs(5))
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => {
                    registry.record_success(&peer.url);
                    debug!(target: "hydrocube::peers", "Health check OK: {}", peer.url);
                }
                Ok(resp) => {
                    warn!(
                        target: "hydrocube::peers",
                        "Health check {} returned HTTP {}",
                        peer.url,
                        resp.status()
                    );
                    registry.record_failure(&peer.url, max_failures);
                }
                Err(e) => {
                    warn!(target: "hydrocube::peers", "Health check {} failed: {}", peer.url, e);
                    registry.record_failure(&peer.url, max_failures);
                }
            }
        }
    }
}
```

- [ ] **Step 2: Confirm the project compiles**

```bash
cargo build --no-default-features 2>&1 | tail -10
```

Expected: builds cleanly.

- [ ] **Step 3: Commit**

```bash
git add src/peers/gossip.rs
git commit -m "feat: implement gossip bootstrap and background health-check loop"
```

---

### Task 5: Add peer API handlers to src/web/api.rs

**Files:**
- Modify: `src/web/api.rs`

- [ ] **Step 1: Add peers import and update AppState**

In `src/web/api.rs`, add to the `use` block at the top:

```rust
use crate::peers::{PeerRecord, PeerRegistry, PeerStatus};
```

Update the `AppState` struct:

```rust
#[derive(Clone)]
pub struct AppState {
    pub db: DbManager,
    pub config: CubeConfig,
    /// Full aggregation SQL with synthetic `_key` column injected.
    pub snapshot_sql: String,
    pub start_time: Instant,
    pub broadcast_tx: broadcast::Sender<DeltaEvent>,
    /// Peer registry; None when the `peers:` config section is absent.
    pub peer_registry: Option<Arc<PeerRegistry>>,
    /// Shared HTTP client for outbound peer API calls (gossip forwarding).
    pub http_client: reqwest::Client,
}
```

- [ ] **Step 2: Add the GET /api/peers handler**

Append to `src/web/api.rs`:

```rust
// ---------------------------------------------------------------------------
// GET /api/peers
// ---------------------------------------------------------------------------

#[derive(Serialize)]
pub struct GetPeersResponse {
    #[serde(rename = "self")]
    pub self_info: PeerRecord,
    pub peers: Vec<PeerRecord>,
}

pub async fn get_peers_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match &state.peer_registry {
        None => {
            // No peers config — return self with empty peers list.
            let self_info = PeerRecord {
                name: state.config.name.clone(),
                url: String::new(),
                description: state.config.description.clone().unwrap_or_default(),
                status: PeerStatus::Online,
            };
            Json(GetPeersResponse {
                self_info,
                peers: vec![],
            })
            .into_response()
        }
        Some(registry) => {
            let self_info = registry.own_info();
            let peers = registry.list();
            Json(GetPeersResponse { self_info, peers }).into_response()
        }
    }
}
```

- [ ] **Step 3: Add the POST /api/peers/register handler**

Append to `src/web/api.rs`:

```rust
// ---------------------------------------------------------------------------
// POST /api/peers/register
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct RegisterPeerRequest {
    pub name: String,
    pub url: String,
    pub description: String,
    /// If true, this is a forwarded registration — do NOT forward further.
    /// Prevents gossip loops.
    #[serde(default)]
    pub forwarded: bool,
}

#[derive(Serialize)]
struct ForwardBody<'a> {
    name: &'a str,
    url: &'a str,
    description: &'a str,
    forwarded: bool,
}

pub async fn register_peer_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<RegisterPeerRequest>,
) -> impl IntoResponse {
    let registry = match &state.peer_registry {
        Some(r) => r.clone(),
        None => return Json(Vec::<PeerRecord>::new()).into_response(),
    };

    // Upsert the registering peer.
    registry.upsert(PeerRecord {
        name: body.name.clone(),
        url: body.url.clone(),
        description: body.description.clone(),
        status: PeerStatus::Online,
    });

    // One-hop forwarding: if this is NOT already a forwarded registration,
    // send it to all other known peers to converge the fleet in one round-trip.
    if !body.forwarded {
        let forward_targets = registry.peer_urls_except(&body.url);
        if !forward_targets.is_empty() {
            let client = state.http_client.clone();
            let name = body.name.clone();
            let url = body.url.clone();
            let description = body.description.clone();
            tokio::spawn(async move {
                for target_base in forward_targets {
                    let forward_url =
                        format!("{}/api/peers/register", target_base.trim_end_matches('/'));
                    let fw = ForwardBody {
                        name: &name,
                        url: &url,
                        description: &description,
                        forwarded: true,
                    };
                    if let Err(e) = client.post(&forward_url).json(&fw).send().await {
                        tracing::warn!(
                            target: "hydrocube::peers",
                            "Failed to forward registration to {}: {}",
                            target_base,
                            e
                        );
                    }
                }
            });
        }
    }

    // Return our current peer list so the caller gets one-round-trip convergence.
    Json(registry.list()).into_response()
}
```

- [ ] **Step 4: Confirm the project compiles**

```bash
cargo build --no-default-features 2>&1 | tail -10
```

Expected: builds cleanly (main.rs will error on the changed `start_server` signature — that's fixed in Task 7).

- [ ] **Step 5: Commit**

```bash
git add src/web/api.rs
git commit -m "feat: add GET /api/peers and POST /api/peers/register handlers"
```

---

### Task 6: Wire peer routes into src/web/server.rs

**Files:**
- Modify: `src/web/server.rs`

- [ ] **Step 1: Update imports**

Add to the `use` block in `src/web/server.rs`:

```rust
use std::sync::Arc;
use crate::peers::PeerRegistry;
use super::api::{
    get_peers_handler, register_peer_handler,
    query_handler, schema_handler, snapshot_handler, status_handler, AppState,
};
```

- [ ] **Step 2: Update start_server signature**

```rust
pub async fn start_server(
    db: DbManager,
    config: CubeConfig,
    broadcast_tx: broadcast::Sender<DeltaEvent>,
    port: u16,
    peer_registry: Option<Arc<PeerRegistry>>,
    http_client: reqwest::Client,
) -> HcResult<()> {
```

- [ ] **Step 3: Update AppState construction inside start_server**

```rust
    let state = Arc::new(AppState {
        db,
        config,
        snapshot_sql,
        start_time: std::time::Instant::now(),
        broadcast_tx,
        peer_registry,
        http_client,
    });
```

- [ ] **Step 4: Add the two new routes to the router**

```rust
    let router = Router::new()
        .route("/api/status", get(status_handler))
        .route("/api/schema", get(schema_handler))
        .route("/api/snapshot", get(snapshot_handler))
        .route("/api/query", post(query_handler))
        .route("/api/peers", get(get_peers_handler))
        .route("/api/peers/register", post(register_peer_handler))
        .route("/api/stream", get(sse_handler))
        .route("/assets/{*path}", get(static_handler))
        .route("/", get(index_handler))
        .with_state(state);
```

- [ ] **Step 5: Confirm the project compiles**

```bash
cargo build --no-default-features 2>&1 | tail -10
```

Expected: builds cleanly (main.rs will error on changed signature — fixed in Task 7).

- [ ] **Step 6: Commit**

```bash
git add src/web/server.rs
git commit -m "feat: wire /api/peers and /api/peers/register routes into axum router"
```

---

### Task 7: Wire peers into main.rs

**Files:**
- Modify: `src/main.rs`

- [ ] **Step 1: Add imports**

Add to the `use hydrocube::...` block:

```rust
use hydrocube::peers::gossip;
use hydrocube::peers::{PeerRecord, PeerRegistry, PeerStatus};
use std::sync::Arc;
```

- [ ] **Step 2: Add peer registry setup after broadcast channel setup (section 12)**

Add a new section `12b` immediately after `// 12. Set up broadcast channel`:

```rust
    // -------------------------------------------------------------------------
    // 12b. Set up peer registry and HTTP client (if peers: config present)
    // -------------------------------------------------------------------------
    let http_client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .build()
        .expect("failed to build reqwest HTTP client");

    let peer_registry: Option<Arc<PeerRegistry>> = if let Some(peers_cfg) = &config.peers {
        let own_info = PeerRecord {
            name: config.name.clone(),
            url: peers_cfg.url.clone(),
            description: peers_cfg.description.clone(),
            status: PeerStatus::Online,
        };
        let registry = Arc::new(PeerRegistry::new(own_info.clone()));

        // Gossip bootstrap — runs once at startup, fire-and-forget.
        let bootstrap_registry = registry.clone();
        let bootstrap_cfg = peers_cfg.clone();
        let bootstrap_client = http_client.clone();
        tokio::spawn(async move {
            gossip::bootstrap(&own_info, &bootstrap_cfg, &bootstrap_registry, &bootstrap_client)
                .await;
        });

        // Health-check loop — runs until process exits.
        let hc_registry = (*registry).clone();
        let hc_cfg = peers_cfg.clone();
        let hc_client = http_client.clone();
        tokio::spawn(async move {
            gossip::run_health_checks(hc_registry, hc_cfg, hc_client).await;
        });

        info!(
            target: "hydrocube",
            "Peer discovery enabled — {} seed(s) configured",
            peers_cfg.seeds.len()
        );
        Some(registry)
    } else {
        info!(target: "hydrocube", "Peer discovery disabled (no peers: config section)");
        None
    };
```

- [ ] **Step 3: Update the start_server call**

Find the `tokio::spawn` that calls `start_server`. Replace the variable setup and spawn:

```rust
        let db_web = db.clone();
        let config_web = config.clone();
        let broadcast_tx_web = broadcast_tx.clone();
        let peer_registry_web = peer_registry.clone();
        let http_client_web = http_client.clone();

        tokio::spawn(async move {
            if let Err(e) =
                start_server(db_web, config_web, broadcast_tx_web, port, peer_registry_web, http_client_web)
                    .await
            {
                error!("Web server error: {}", e);
            }
        });
```

- [ ] **Step 4: Confirm the project compiles**

```bash
cargo build --no-default-features 2>&1 | tail -10
```

Expected: builds cleanly.

- [ ] **Step 5: Run all tests**

```bash
cargo test --no-default-features 2>&1 | tail -20
```

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/main.rs
git commit -m "feat: wire peer registry and gossip tasks into main startup sequence"
```

---

### Task 8: Dynamic cube selector in index.html.j2

**Files:**
- Modify: `templates/index.html.j2`

- [ ] **Step 1: Replace the static cube selector with a dynamic one**

Find this block (around line 191):

```html
  <!-- Cube selector -->
  <div class="dropdown ms-1">
    <button class="nav-btn dropdown-toggle" data-bs-toggle="dropdown">
      ⬡ {{ cube_name }}
    </button>
    <ul class="dropdown-menu shadow-sm">
      <li><a class="dropdown-item active" href="#">{{ cube_name }}</a></li>
      <li><hr class="dropdown-divider"></li>
      <li><a class="dropdown-item text-muted" href="#"><em>Connect to another cube…</em></a></li>
    </ul>
  </div>
```

Replace with:

```html
  <!-- Cube selector — populated dynamically from /api/peers -->
  <div class="dropdown ms-1" id="cube-selector-dropdown">
    <button class="nav-btn dropdown-toggle" data-bs-toggle="dropdown" id="cube-selector-btn">
      ⬡ {{ cube_name }}
    </button>
    <ul class="dropdown-menu shadow-sm" id="cube-selector-menu">
      <li><a class="dropdown-item active" href="#">⬡ {{ cube_name }}</a></li>
    </ul>
  </div>
```

- [ ] **Step 2: Add CSS for offline peer entries**

Inside the `<style>` block, after the `.nav-sep` rule:

```css
    /* Offline peer entries in cube selector */
    .peer-offline {
      display: block;
      padding: .25rem 1rem;
      font-size: .875rem;
      opacity: 0.45;
      cursor: default;
      user-select: none;
    }
    .peer-offline::after {
      content: ' ⚠';
      font-size: .75em;
    }
```

- [ ] **Step 3: Add loadPeers() to the script block**

In the `<script>` section, add the following at the very top of the script (before any other code):

```js
  // ── Peer discovery ────────────────────────────────────────────────────────
  async function loadPeers() {
    let data;
    try {
      const resp = await fetch('/api/peers');
      if (resp.status === 401) { showNotAuthorised(); return; }
      if (!resp.ok) return;
      data = await resp.json();
    } catch (_) {
      return; // Network error — leave static fallback in place.
    }

    const menu = document.getElementById('cube-selector-menu');
    if (!menu || !data) return;

    menu.innerHTML = '';

    // Current cube — always first, always active, not a link.
    const selfLi = document.createElement('li');
    selfLi.innerHTML = `<a class="dropdown-item active" href="#">⬡ ${data.self.name}</a>`;
    menu.appendChild(selfLi);

    if (data.peers && data.peers.length > 0) {
      const divLi = document.createElement('li');
      divLi.innerHTML = '<hr class="dropdown-divider">';
      menu.appendChild(divLi);

      for (const peer of data.peers) {
        const li = document.createElement('li');
        const label = peer.description || peer.name;
        if (peer.status === 'offline') {
          li.innerHTML = `<span class="peer-offline">⬡ ${label}</span>`;
        } else {
          li.innerHTML = `<a class="dropdown-item" href="${peer.url}">⬡ ${label}</a>`;
        }
        menu.appendChild(li);
      }
    }
  }
```

- [ ] **Step 4: Build and verify**

```bash
cargo build --no-default-features 2>&1 | tail -5
```

Expected: builds cleanly.

- [ ] **Step 5: Commit**

```bash
git add templates/index.html.j2
git commit -m "feat: dynamic cube selector populated from /api/peers"
```

---

### Task 9: Not-authorised overlay in index.html.j2

When any API fetch returns HTTP 401, Perspective initialisation is skipped and a full-height overlay is shown. This requires no auth middleware changes — it activates automatically once auth is wired.

**Files:**
- Modify: `templates/index.html.j2`

- [ ] **Step 1: Add overlay HTML**

Inside `<div id="perspective-wrapper">`, add as the first child (before `#psp-zoom-host`):

```html
    <!-- Not-authorised overlay — hidden until showNotAuthorised() is called -->
    <div id="not-authorised-overlay"
         style="display:none; position:absolute; inset:0; z-index:200;
                background:var(--bs-body-bg,#fff); align-items:center;
                justify-content:center; flex-direction:column; gap:12px;
                text-align:center; padding:40px;">
      <svg xmlns="http://www.w3.org/2000/svg" width="48" height="48" fill="none"
           viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5"
           style="color:#6c757d">
        <path stroke-linecap="round" stroke-linejoin="round"
              d="M12 9v4m0 4h.01M10.29 3.86L1.82 18a2 2 0 001.71
                 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z"/>
      </svg>
      <h5 style="margin:0;color:#343a40">Not Authorised</h5>
      <p style="margin:0;color:#6c757d;max-width:360px">
        You are not authorised to access this cube.<br>
        Contact your administrator.
      </p>
    </div>
```

- [ ] **Step 2: Add showNotAuthorised() to the script block**

Add immediately after the `loadPeers` function:

```js
  // ── Auth overlay ─────────────────────────────────────────────────────────
  let _notAuthorised = false;

  function showNotAuthorised() {
    if (_notAuthorised) return;
    _notAuthorised = true;
    const overlay = document.getElementById('not-authorised-overlay');
    if (overlay) overlay.style.display = 'flex';
  }
```

- [ ] **Step 3: Wrap Perspective initialisation in an async IIFE guarded by auth**

Find the existing Perspective initialisation code. It will be inside or following `<script>`. Wrap the entire init sequence:

```js
  (async () => {
    // Load peers first — if we get a 401, showNotAuthorised() is called
    // and _notAuthorised is set before we attempt Perspective init.
    await loadPeers();
    if (_notAuthorised) return;

    // --- existing Perspective init code begins here ---
    // ...
    // --- existing Perspective init code ends here ---
  })();
```

Remove any standalone `loadPeers()` call that was added in Task 8 — it is now inside this IIFE.

- [ ] **Step 4: Add 401 check to the snapshot fetch**

Find the `fetch('/api/snapshot')` call and add an auth guard:

```js
    const snapResp = await fetch('/api/snapshot');
    if (snapResp.status === 401) { showNotAuthorised(); return; }
    if (!snapResp.ok) throw new Error('Snapshot fetch failed: ' + snapResp.status);
```

- [ ] **Step 5: Build and verify**

```bash
cargo build --no-default-features 2>&1 | tail -5
```

Expected: builds cleanly.

- [ ] **Step 6: Commit**

```bash
git add templates/index.html.j2
git commit -m "feat: not-authorised overlay — shown on 401, guards Perspective init"
```

---

### Task 10: End-to-end smoke test with two local processes

Validates the full gossip flow using the development binary.

- [ ] **Step 1: Create a second test config**

Create `/tmp/cube2.yaml`:

```yaml
name: risk_cube
description: Risk dashboard

source:
  type: test
schema:
  columns:
    - name: id
      type: VARCHAR
    - name: value
      type: DOUBLE
aggregation:
  sql: "SELECT id, SUM(value) AS total FROM slices GROUP BY id"
window:
  interval_ms: 2000
compaction:
  interval_windows: 60
retention:
  duration: 1h
  parquet_path: /tmp/hc_risk_parquet
persistence:
  enabled: false
  path: ":memory:"
  flush_interval: 10
ui:
  enabled: true
  port: 8081
peers:
  url: "http://localhost:8081"
  description: "Risk dashboard"
  seeds:
    - "http://localhost:8080"
```

Add a `peers:` section to `cube.example.yaml` (or create a local copy):

```yaml
peers:
  url: "http://localhost:8080"
  description: "Live trading positions"
  seeds:
    - "http://localhost:8081"
```

- [ ] **Step 2: Start both processes**

```bash
cargo build --no-default-features
./target/debug/hydrocube --config cube.example.yaml &
./target/debug/hydrocube --config /tmp/cube2.yaml &
sleep 3
```

- [ ] **Step 3: Verify gossip convergence**

```bash
curl -s http://localhost:8080/api/peers | python3 -m json.tool
```

Expected: `peers` array contains `risk_cube` with `"status": "online"`.

```bash
curl -s http://localhost:8081/api/peers | python3 -m json.tool
```

Expected: `peers` array contains the positions cube with `"status": "online"`.

- [ ] **Step 4: Verify the UI cube selector**

Open `http://localhost:8080` in a browser. Click the ⬡ dropdown. Expected: "⬡ Risk dashboard" appears as a clickable link to `http://localhost:8081`.

- [ ] **Step 5: Verify offline detection**

```bash
kill %2        # stop cube 2
sleep 35       # wait past health-check interval
curl -s http://localhost:8080/api/peers | python3 -m json.tool
```

Expected: `risk_cube` shows `"status": "offline"`. In the UI the entry is greyed out with ⚠.

- [ ] **Step 6: Stop cube 1 and run full test suite**

```bash
kill %1
cargo test --no-default-features 2>&1 | tail -20
```

Expected: all tests pass.

- [ ] **Step 7: Final formatting and commit**

```bash
cargo fmt --all
git add -A
git commit -m "chore: fmt — multi-cube peer discovery complete"
```

---

## Self-Review

**Spec coverage:**

| Spec requirement | Task |
|---|---|
| One process per cube | Architectural — no code needed |
| `GET /api/peers` | Task 5 |
| `POST /api/peers/register` | Task 5 |
| Peer record: name, url, description, status | Task 3 |
| Gossip bootstrap: GET peers from seeds | Task 4 |
| Gossip bootstrap: POST register to seeds | Task 4 |
| One-hop forwarding on registration | Task 5 (`forwarded` flag) |
| Background health checks every N seconds | Task 4 |
| Mark offline after N consecutive failures | Tasks 3 + 4 |
| Recover online automatically | Tasks 3 + 4 |
| `peers:` config: url, description, seeds, health-check settings | Task 2 |
| Standalone mode when `peers:` absent | Tasks 3, 5, 6, 7 |
| Dynamic cube selector dropdown | Task 8 |
| Online peers → clickable links (URL-swap nav) | Task 8 |
| Offline peers → greyed out with ⚠ | Task 8 |
| Not-authorised overlay on 401 | Task 9 |
| No Perspective init when not authorised | Task 9 |
| Existing deployments unaffected | Task 2 (`#[serde(default)]`), Task 7 |

**Placeholder scan:** None found.

**Type consistency:** `PeerRecord` defined in Task 3, used identically across Tasks 4, 5, 6, 7, 8. `PeerRegistry` methods defined in Task 3, called with matching signatures in Tasks 5 and 7. `GetPeersResponse` (Task 5 handler) matches `GetPeersResponse` deserialization struct in Task 4 gossip (same `self`/`peers` field names). `forwarded: bool` default-false in `RegisterPeerRequest` (Task 5) matches `forwarded: true` sent by the forwarding loop in the same task. ✓

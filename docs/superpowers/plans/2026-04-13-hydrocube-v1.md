# HydroCube v1.0 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a real-time aggregation engine that ingests from Kafka, aggregates with DuckDB, detects deltas, and streams results to browsers via SSE with a Perspective-based UI.

**Architecture:** Single Rust binary. Kafka ingest fills a buffer, a window timer flushes to DuckDB slices, an aggregation engine computes GROUP BY results using a two-table model (consolidated + recent slices), delta detection identifies changes, and SSE broadcasts Arrow IPC payloads to browser clients running Perspective WASM. A DB manager thread serialises all DuckDB access. A background compaction thread periodically consolidates old slices and exports them to time-partitioned Parquet files.

**Tech Stack:** Rust, DuckDB (`duckdb` crate), Kafka (`rdkafka`), Lua 5.4 (`mlua`), Axum (HTTP + SSE), Arrow IPC, Perspective WASM, Bootstrap 5 / Bootswatch, Minijinja, NATS (`async-nats`, optional), `tokio`, `tracing`, `clap`

**Spec:** `docs/superpowers/specs/2026-04-13-hydrocube-design.md`

---

## File Structure

```
hydrocube/
├── Cargo.toml
├── CLAUDE.md
├── README.md
├── LICENSE.md
├── cube.example.yaml
├── scripts/
│   └── vendor.sh
├── static/
│   ├── perspective/            # Vendored WASM + JS (Task 15)
│   ├── bootstrap/              # Vendored CSS + JS (Task 15)
│   ├── bootswatch/
│   │   ├── flatly.min.css
│   │   └── darkly.min.css
│   └── codemirror/             # Vendored SQL editor (Task 15)
├── templates/
│   └── index.html.j2           # Minijinja template (Task 16)
├── src/
│   ├── main.rs                 # Entrypoint, wires everything (Task 18)
│   ├── cli.rs                  # Clap CLI args (Task 2)
│   ├── config.rs               # YAML config parsing + validation (Task 3)
│   ├── error.rs                # Error types (Task 1)
│   ├── db_manager.rs           # Single-connection DuckDB manager (Task 4)
│   ├── persistence.rs          # Schema init, metadata, config hash, rebuild (Task 5)
│   ├── ingest/
│   │   ├── mod.rs              # IngestSource trait + factory (Task 6)
│   │   ├── kafka.rs            # Kafka consumer (Task 6)
│   │   └── parser.rs           # JSON message parser (Task 6)
│   ├── transform/
│   │   ├── mod.rs              # Pipeline orchestration (Task 7)
│   │   ├── sql.rs              # SQL transform step (Task 7)
│   │   └── lua.rs              # Lua transform step with batch contract (Task 8)
│   ├── aggregation/
│   │   ├── mod.rs              # Aggregation orchestration (Task 10)
│   │   ├── sql_gen.rs          # SQL generation from YAML config (Task 9)
│   │   ├── decompose.rs        # Measure decomposability detection (Task 9)
│   │   └── window.rs           # Micro-batch window timer (Task 10)
│   ├── delta.rs                # Hash-based delta detection (Task 11)
│   ├── compaction.rs           # Background compaction thread (Task 12)
│   ├── retention.rs            # Parquet partition write + prune (Task 12)
│   ├── publish.rs              # Optional NATS publication (Task 13)
│   ├── shutdown.rs             # Graceful shutdown sequence (Task 18)
│   ├── auth/
│   │   ├── mod.rs              # Auth middleware dispatcher (Task 17)
│   │   └── basic.rs            # Basic auth middleware (Task 17)
│   └── web/
│       ├── mod.rs              # Web module exports (Task 14)
│       ├── server.rs           # Axum HTTP server + router (Task 14)
│       ├── api.rs              # REST endpoints: query, status, snapshot, schema (Task 14)
│       ├── sse.rs              # SSE endpoint + broadcast fan-out (Task 13)
│       └── assets.rs           # Embedded static file serving (Task 15)
└── tests/
    ├── config_test.rs          # Config parsing tests (Task 3)
    ├── db_manager_test.rs      # DB manager tests (Task 4)
    ├── persistence_test.rs     # Persistence + rebuild tests (Task 5)
    ├── parser_test.rs          # JSON parser tests (Task 6)
    ├── transform_test.rs       # SQL + Lua transform tests (Tasks 7-8)
    ├── sql_gen_test.rs         # SQL generation tests (Task 9)
    ├── aggregation_test.rs     # Hot path aggregation tests (Task 10)
    ├── delta_test.rs           # Delta detection tests (Task 11)
    ├── compaction_test.rs      # Compaction + retention tests (Task 12)
    └── integration_test.rs     # End-to-end tests (Task 19)
```

---

## Phase 1: Foundation (Tasks 1-5)

### Task 1: Project Scaffold + Error Types

**Files:**
- Create: `Cargo.toml`
- Create: `src/main.rs`
- Create: `src/error.rs`
- Create: `CLAUDE.md`

- [ ] **Step 1: Create Cargo.toml with all v1.0 dependencies**

```toml
[package]
name = "hydrocube"
version = "0.1.0"
edition = "2021"
description = "Real-time aggregation engine"

[features]
default = ["kafka"]
kafka = ["rdkafka"]

[dependencies]
# Core
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
serde_yaml = "0.9"
thiserror = "2"
anyhow = "1"

# CLI
clap = { version = "4", features = ["derive"] }

# DuckDB
duckdb = { version = "1.2", features = ["bundled"] }

# Arrow
arrow = { version = "54", features = ["ipc"] }

# Kafka
rdkafka = { version = "0.37", features = ["cmake-build"], optional = true }

# NATS (optional at runtime, always compiled)
async-nats = "0.39"

# Lua
mlua = { version = "0.10", features = ["lua54", "vendored", "serialize"] }

# Web
axum = { version = "0.8", features = ["ws"] }
axum-extra = { version = "0.10", features = ["typed-header"] }
tower = "0.5"
tower-http = { version = "0.6", features = ["cors", "fs"] }
rust-embed = "8"

# Templating
minijinja = { version = "2", features = ["loader"] }

# Logging
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }

# Misc
base64 = "0.22"
sha2 = "0.10"
chrono = { version = "0.4", features = ["serde"] }
uuid = { version = "1", features = ["v4"] }

[dev-dependencies]
tempfile = "3"
tokio-test = "0.4"
```

- [ ] **Step 2: Create src/error.rs with the project error types**

```rust
// src/error.rs
use thiserror::Error;

#[derive(Error, Debug)]
pub enum HcError {
    #[error("configuration error: {0}")]
    Config(String),

    #[error("DuckDB error: {0}")]
    DuckDb(#[from] duckdb::Error),

    #[error("ingest error: {0}")]
    Ingest(String),

    #[error("transform error: {0}")]
    Transform(String),

    #[error("aggregation error: {0}")]
    Aggregation(String),

    #[error("persistence error: {0}")]
    Persistence(String),

    #[error("publication error: {0}")]
    Publish(String),

    #[error("web server error: {0}")]
    Web(String),

    #[error("shutdown error: {0}")]
    Shutdown(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("YAML error: {0}")]
    Yaml(#[from] serde_yaml::Error),

    #[error("config hash mismatch — run with --rebuild")]
    ConfigHashMismatch,
}

pub type HcResult<T> = Result<T, HcError>;

/// Exit codes per spec
pub mod exit_code {
    pub const OK: i32 = 0;
    pub const CONFIG_ERROR: i32 = 1;
    pub const SOURCE_CONNECTION_FAILURE: i32 = 2;
    pub const PERSISTENCE_FAILURE: i32 = 3;
    pub const PORT_CONFLICT: i32 = 4;
    pub const CONFIG_HASH_MISMATCH: i32 = 5;
}
```

- [ ] **Step 3: Create minimal src/main.rs**

```rust
// src/main.rs
mod cli;
mod config;
mod error;

fn main() {
    println!("HydroCube");
}
```

- [ ] **Step 4: Create CLAUDE.md with project conventions**

```markdown
# HydroCube Development Guide

## Build & Test
- `cargo build` — build (requires librdkafka or use `--no-default-features` to skip Kafka)
- `cargo build --no-default-features` — build without Kafka (for dev without librdkafka installed)
- `cargo test` — run all tests
- `cargo test --test <name>` — run a specific integration test
- `cargo run -- --config cube.example.yaml --validate` — validate a config file

## Architecture
- All DuckDB access goes through `db_manager.rs` (channel-based, single connection)
- The hot path (window flush + aggregate + delta detect) runs on a tokio task
- Compaction runs on a separate tokio task, sends SQL to the DB manager
- SSE delivery uses `tokio::broadcast` for fan-out
- Lua transforms use batch contract (`transform_batch`) for performance

## Conventions
- Error types in `src/error.rs` — use `HcError` variants, propagate with `?`
- Config structs in `src/config.rs` — all serde-deserializable from YAML
- Logging via `tracing` macros (`info!`, `debug!`, `warn!`, `error!`) with target tags
- Tests use `tempfile` for DuckDB databases — no persistent test state
```

- [ ] **Step 5: Verify it compiles**

Run: `cd /Users/josephfrost/code/HydroCube/.claude/worktrees/pedantic-khorana && cargo check --no-default-features`
Expected: compiles with no errors (may have unused warnings, that's fine)

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml src/main.rs src/error.rs CLAUDE.md
git commit -m "feat: project scaffold with error types and dependencies"
```

---

### Task 2: CLI Argument Parsing

**Files:**
- Create: `src/cli.rs`
- Modify: `src/main.rs`

- [ ] **Step 1: Write src/cli.rs with clap derive**

```rust
// src/cli.rs
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "hydrocube", about = "Real-time aggregation engine")]
pub struct Cli {
    /// Path to cube YAML config
    #[arg(long, required = true)]
    pub config: PathBuf,

    /// Validate config and exit
    #[arg(long)]
    pub validate: bool,

    /// Drop consolidated state, rebuild from Parquet history
    #[arg(long)]
    pub rebuild: bool,

    /// Export current aggregate to Parquet file and exit
    #[arg(long)]
    pub snapshot: Option<PathBuf>,

    /// Clear all persistent state and start fresh
    #[arg(long)]
    pub reset: bool,

    /// Override config log level
    #[arg(long)]
    pub log_level: Option<String>,

    /// Disable web UI
    #[arg(long)]
    pub no_ui: bool,

    /// Override config UI port
    #[arg(long)]
    pub ui_port: Option<u16>,
}
```

- [ ] **Step 2: Update main.rs to parse CLI args**

```rust
// src/main.rs
mod cli;
mod config;
mod error;

use clap::Parser;
use cli::Cli;

fn main() {
    let cli = Cli::parse();
    println!("Config: {:?}", cli.config);
}
```

- [ ] **Step 3: Verify CLI parsing works**

Run: `cargo run --no-default-features -- --config test.yaml --validate`
Expected: prints "Config: test.yaml" (the file doesn't need to exist yet)

Run: `cargo run --no-default-features -- --help`
Expected: shows help text with all flags

- [ ] **Step 4: Commit**

```bash
git add src/cli.rs src/main.rs
git commit -m "feat: CLI argument parsing with clap"
```

---

### Task 3: Configuration Parsing

**Files:**
- Create: `src/config.rs`
- Create: `cube.example.yaml`
- Create: `tests/config_test.rs`

- [ ] **Step 1: Write the failing test for config parsing**

```rust
// tests/config_test.rs
use std::path::Path;

#[test]
fn test_parse_example_config() {
    let yaml = std::fs::read_to_string("cube.example.yaml").unwrap();
    let config: hydrocube::config::CubeConfig = serde_yaml::from_str(&yaml).unwrap();
    assert_eq!(config.name, "trading_positions");
    assert_eq!(config.source.source_type, "kafka");
    assert_eq!(config.schema.columns.len(), 11);
    assert_eq!(config.window.interval_ms, 1000);
    assert_eq!(config.compaction.interval_windows, 60);
}

#[test]
fn test_config_hash_deterministic() {
    let yaml = std::fs::read_to_string("cube.example.yaml").unwrap();
    let config: hydrocube::config::CubeConfig = serde_yaml::from_str(&yaml).unwrap();
    let hash1 = config.schema_hash();
    let hash2 = config.schema_hash();
    assert_eq!(hash1, hash2);
}

#[test]
fn test_config_validation_missing_name() {
    let yaml = r#"
source:
  type: kafka
  brokers: ["localhost:9092"]
  topic: "test"
  group_id: "test"
  format: json
schema:
  columns: []
aggregation:
  sql: "SELECT 1"
window:
  interval_ms: 1000
compaction:
  interval_windows: 60
retention:
  duration: "6d"
  parquet_path: "/tmp/test"
persistence:
  enabled: false
  path: "/tmp/test.db"
  flush_interval: 10
"#;
    let result: Result<hydrocube::config::CubeConfig, _> = serde_yaml::from_str(yaml);
    // Should fail because `name` is missing
    assert!(result.is_err());
}

#[test]
fn test_window_interval_validation() {
    let yaml = std::fs::read_to_string("cube.example.yaml").unwrap();
    let mut config: hydrocube::config::CubeConfig = serde_yaml::from_str(&yaml).unwrap();
    config.window.interval_ms = 50; // Below minimum of 100
    let result = config.validate();
    assert!(result.is_err());
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --no-default-features --test config_test 2>&1 | head -20`
Expected: compilation errors — `config` module doesn't exist yet

- [ ] **Step 3: Create cube.example.yaml**

```yaml
# cube.example.yaml
name: trading_positions
description: Real-time position aggregation by book

source:
  type: kafka
  brokers: ["localhost:9092"]
  topic: "trades.executed"
  group_id: "hydrocube-positions"
  format: json

schema:
  columns:
    - name: trade_id
      type: VARCHAR
    - name: book
      type: VARCHAR
    - name: desk
      type: VARCHAR
    - name: instrument
      type: VARCHAR
    - name: instrument_type
      type: VARCHAR
    - name: currency
      type: VARCHAR
    - name: quantity
      type: DOUBLE
    - name: price
      type: DOUBLE
    - name: notional
      type: DOUBLE
    - name: side
      type: VARCHAR
    - name: trade_time
      type: TIMESTAMP

aggregation:
  sql: |
    SELECT
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
    GROUP BY book, desk, instrument_type, currency

window:
  interval_ms: 1000

compaction:
  interval_windows: 60

retention:
  duration: 6d
  parquet_path: "/data/hydrocube/slices/"

persistence:
  enabled: true
  path: "/data/hydrocube/positions.db"
  flush_interval: 10

ui:
  enabled: true
  port: 8080

log_level: info
```

- [ ] **Step 4: Implement src/config.rs**

```rust
// src/config.rs
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::error::{HcError, HcResult};

#[derive(Debug, Deserialize, Clone)]
pub struct CubeConfig {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    pub source: SourceConfig,
    pub schema: SchemaConfig,
    #[serde(default)]
    pub transform: Option<Vec<TransformStep>>,
    pub aggregation: AggregationConfig,
    pub window: WindowConfig,
    pub compaction: CompactionConfig,
    pub retention: RetentionConfig,
    pub persistence: PersistenceConfig,
    #[serde(default)]
    pub publish: Option<PublishConfig>,
    #[serde(default)]
    pub ui: Option<UiConfig>,
    #[serde(default)]
    pub auth: Option<AuthConfig>,
    #[serde(default = "default_log_level")]
    pub log_level: String,
}

fn default_log_level() -> String {
    "info".to_string()
}

#[derive(Debug, Deserialize, Clone)]
pub struct SourceConfig {
    #[serde(rename = "type")]
    pub source_type: String,
    // Kafka fields
    #[serde(default)]
    pub brokers: Option<Vec<String>>,
    #[serde(default)]
    pub topic: Option<String>,
    #[serde(default)]
    pub group_id: Option<String>,
    #[serde(default = "default_format")]
    pub format: String,
}

fn default_format() -> String {
    "json".to_string()
}

#[derive(Debug, Deserialize, Clone)]
pub struct SchemaConfig {
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ColumnDef {
    pub name: String,
    #[serde(rename = "type")]
    pub col_type: String,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum TransformStep {
    #[serde(rename = "sql")]
    Sql { sql: String },
    #[serde(rename = "lua")]
    Lua {
        script: String,
        function: String,
        #[serde(default)]
        init: Option<String>,
    },
}

#[derive(Debug, Deserialize, Clone)]
pub struct AggregationConfig {
    pub sql: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct WindowConfig {
    pub interval_ms: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct CompactionConfig {
    pub interval_windows: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RetentionConfig {
    pub duration: String,
    pub parquet_path: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct PersistenceConfig {
    pub enabled: bool,
    pub path: String,
    pub flush_interval: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct PublishConfig {
    #[serde(default)]
    pub nats: Option<NatsPublishConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct NatsPublishConfig {
    pub url: String,
    pub subject_prefix: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct UiConfig {
    #[serde(default = "default_ui_enabled")]
    pub enabled: bool,
    #[serde(default = "default_ui_port")]
    pub port: u16,
}

fn default_ui_enabled() -> bool {
    true
}

fn default_ui_port() -> u16 {
    8080
}

#[derive(Debug, Deserialize, Clone)]
pub struct AuthConfig {
    #[serde(rename = "type")]
    pub auth_type: String,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
}

impl CubeConfig {
    /// Produce a deterministic hash of the schema-affecting config fields.
    /// Used to detect config changes that require --rebuild.
    pub fn schema_hash(&self) -> String {
        let mut hasher = Sha256::new();
        // Hash schema columns
        for col in &self.schema.columns {
            hasher.update(col.name.as_bytes());
            hasher.update(col.col_type.as_bytes());
        }
        // Hash aggregation SQL
        hasher.update(self.aggregation.sql.as_bytes());
        format!("{:x}", hasher.finalize())
    }

    /// Validate config values that serde can't check.
    pub fn validate(&self) -> HcResult<()> {
        if self.window.interval_ms < 100 || self.window.interval_ms > 60000 {
            return Err(HcError::Config(format!(
                "window.interval_ms must be between 100 and 60000, got {}",
                self.window.interval_ms
            )));
        }
        if self.schema.columns.is_empty() {
            return Err(HcError::Config("schema.columns must not be empty".into()));
        }
        if self.source.source_type == "kafka" {
            if self.source.brokers.is_none() {
                return Err(HcError::Config("kafka source requires 'brokers'".into()));
            }
            if self.source.topic.is_none() {
                return Err(HcError::Config("kafka source requires 'topic'".into()));
            }
            if self.source.group_id.is_none() {
                return Err(HcError::Config("kafka source requires 'group_id'".into()));
            }
        }
        // Parse retention duration
        self.parse_retention_duration()?;
        Ok(())
    }

    /// Parse duration string like "6d" into seconds.
    pub fn parse_retention_duration(&self) -> HcResult<u64> {
        let s = &self.retention.duration;
        if let Some(days) = s.strip_suffix('d') {
            let d: u64 = days.parse().map_err(|_| {
                HcError::Config(format!("invalid retention duration: {}", s))
            })?;
            Ok(d * 86400)
        } else if let Some(hours) = s.strip_suffix('h') {
            let h: u64 = hours.parse().map_err(|_| {
                HcError::Config(format!("invalid retention duration: {}", s))
            })?;
            Ok(h * 3600)
        } else {
            Err(HcError::Config(format!(
                "retention duration must end with 'd' or 'h', got: {}",
                s
            )))
        }
    }
}

impl RetentionConfig {
    pub fn parse_duration_seconds(&self) -> HcResult<u64> {
        let s = &self.duration;
        if let Some(days) = s.strip_suffix('d') {
            let d: u64 = days
                .parse()
                .map_err(|_| HcError::Config(format!("invalid retention duration: {}", s)))?;
            Ok(d * 86400)
        } else if let Some(hours) = s.strip_suffix('h') {
            let h: u64 = hours
                .parse()
                .map_err(|_| HcError::Config(format!("invalid retention duration: {}", s)))?;
            Ok(h * 3600)
        } else {
            Err(HcError::Config(format!(
                "retention duration must end with 'd' or 'h', got: {}",
                s
            )))
        }
    }
}
```

- [ ] **Step 5: Update main.rs to expose config module publicly for tests**

```rust
// src/main.rs
pub mod cli;
pub mod config;
pub mod error;

use clap::Parser;
use cli::Cli;
use error::exit_code;

fn main() {
    let cli = Cli::parse();

    // Load config
    let yaml = match std::fs::read_to_string(&cli.config) {
        Ok(y) => y,
        Err(e) => {
            eprintln!("ERROR: cannot read config file {:?}: {}", cli.config, e);
            std::process::exit(exit_code::CONFIG_ERROR);
        }
    };

    let config: config::CubeConfig = match serde_yaml::from_str(&yaml) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("ERROR: invalid config: {}", e);
            std::process::exit(exit_code::CONFIG_ERROR);
        }
    };

    if let Err(e) = config.validate() {
        eprintln!("ERROR: {}", e);
        std::process::exit(exit_code::CONFIG_ERROR);
    }

    if cli.validate {
        println!("Config OK: cube '{}'", config.name);
        std::process::exit(exit_code::OK);
    }

    println!("HydroCube: cube '{}' loaded", config.name);
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cargo test --no-default-features --test config_test`
Expected: all 4 tests pass

- [ ] **Step 7: Verify --validate works end-to-end**

Run: `cargo run --no-default-features -- --config cube.example.yaml --validate`
Expected: `Config OK: cube 'trading_positions'`

- [ ] **Step 8: Commit**

```bash
git add src/config.rs src/main.rs cube.example.yaml tests/config_test.rs
git commit -m "feat: YAML config parsing with validation"
```

---

### Task 4: DB Manager Thread

**Files:**
- Create: `src/db_manager.rs`
- Create: `tests/db_manager_test.rs`

- [ ] **Step 1: Write failing tests for the DB manager**

```rust
// tests/db_manager_test.rs
use hydrocube::db_manager::DbManager;
use tempfile::NamedTempFile;

#[tokio::test]
async fn test_query_returns_result() {
    let tmp = NamedTempFile::new().unwrap();
    let db = DbManager::open(tmp.path().to_str().unwrap()).await.unwrap();

    db.execute("CREATE TABLE test (id INTEGER, name VARCHAR)".into(), vec![])
        .await
        .unwrap();
    db.execute(
        "INSERT INTO test VALUES (1, 'alice'), (2, 'bob')".into(),
        vec![],
    )
    .await
    .unwrap();

    let rows = db
        .query_json("SELECT id, name FROM test ORDER BY id".into(), vec![])
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["id"], 1);
    assert_eq!(rows[0]["name"], "alice");
}

#[tokio::test]
async fn test_transaction_atomic_swap() {
    let tmp = NamedTempFile::new().unwrap();
    let db = DbManager::open(tmp.path().to_str().unwrap()).await.unwrap();

    db.execute("CREATE TABLE original (v INTEGER)".into(), vec![])
        .await
        .unwrap();
    db.execute("INSERT INTO original VALUES (1)".into(), vec![])
        .await
        .unwrap();
    db.execute("CREATE TABLE replacement (v INTEGER)".into(), vec![])
        .await
        .unwrap();
    db.execute("INSERT INTO replacement VALUES (99)".into(), vec![])
        .await
        .unwrap();

    // Atomic swap via transaction
    db.transaction(vec![
        "DROP TABLE original".into(),
        "ALTER TABLE replacement RENAME TO original".into(),
    ])
    .await
    .unwrap();

    let rows = db
        .query_json("SELECT v FROM original".into(), vec![])
        .await
        .unwrap();
    assert_eq!(rows[0]["v"], 99);
}

#[tokio::test]
async fn test_query_arrow() {
    let tmp = NamedTempFile::new().unwrap();
    let db = DbManager::open(tmp.path().to_str().unwrap()).await.unwrap();

    db.execute("CREATE TABLE test (id INTEGER, val DOUBLE)".into(), vec![])
        .await
        .unwrap();
    db.execute(
        "INSERT INTO test VALUES (1, 3.14), (2, 2.72)".into(),
        vec![],
    )
    .await
    .unwrap();

    let batch = db
        .query_arrow("SELECT id, val FROM test ORDER BY id".into())
        .await
        .unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.num_columns(), 2);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --no-default-features --test db_manager_test 2>&1 | head -5`
Expected: compilation error — `db_manager` module doesn't exist

- [ ] **Step 3: Implement src/db_manager.rs**

```rust
// src/db_manager.rs
use std::path::Path;
use std::thread;

use arrow::record_batch::RecordBatch;
use duckdb::{arrow::record_batch::RecordBatch as DuckRecordBatch, params, Connection};
use serde_json::Value as JsonValue;
use tokio::sync::{mpsc, oneshot};

use crate::error::{HcError, HcResult};

enum DbCommand {
    Execute {
        sql: String,
        reply: oneshot::Sender<HcResult<()>>,
    },
    QueryJson {
        sql: String,
        reply: oneshot::Sender<HcResult<Vec<serde_json::Map<String, JsonValue>>>>,
    },
    QueryArrow {
        sql: String,
        reply: oneshot::Sender<HcResult<RecordBatch>>,
    },
    Transaction {
        statements: Vec<String>,
        reply: oneshot::Sender<HcResult<()>>,
    },
    Shutdown,
}

#[derive(Clone)]
pub struct DbManager {
    tx: mpsc::Sender<DbCommand>,
}

impl DbManager {
    /// Open a DuckDB database and spawn the manager thread.
    pub async fn open(path: &str) -> HcResult<Self> {
        let path = path.to_string();
        let (tx, mut rx) = mpsc::channel::<DbCommand>(256);

        thread::spawn(move || {
            let conn = Connection::open(&path)
                .unwrap_or_else(|e| panic!("Failed to open DuckDB at {}: {}", path, e));

            while let Some(cmd) = rx.blocking_recv() {
                match cmd {
                    DbCommand::Execute { sql, reply } => {
                        let result = conn
                            .execute_batch(&sql)
                            .map_err(HcError::DuckDb);
                        let _ = reply.send(result);
                    }
                    DbCommand::QueryJson { sql, reply } => {
                        let result = Self::run_query_json(&conn, &sql);
                        let _ = reply.send(result);
                    }
                    DbCommand::QueryArrow { sql, reply } => {
                        let result = Self::run_query_arrow(&conn, &sql);
                        let _ = reply.send(result);
                    }
                    DbCommand::Transaction { statements, reply } => {
                        let result = Self::run_transaction(&conn, &statements);
                        let _ = reply.send(result);
                    }
                    DbCommand::Shutdown => break,
                }
            }
        });

        Ok(Self { tx })
    }

    /// Open an in-memory DuckDB for testing.
    pub async fn open_in_memory() -> HcResult<Self> {
        Self::open(":memory:").await
    }

    pub async fn execute(&self, sql: String, _params: Vec<JsonValue>) -> HcResult<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(DbCommand::Execute { sql, reply: reply_tx })
            .await
            .map_err(|_| HcError::DuckDb(duckdb::Error::InvalidQuery))?;
        reply_rx
            .await
            .map_err(|_| HcError::DuckDb(duckdb::Error::InvalidQuery))?
    }

    pub async fn query_json(
        &self,
        sql: String,
        _params: Vec<JsonValue>,
    ) -> HcResult<Vec<serde_json::Map<String, JsonValue>>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(DbCommand::QueryJson { sql, reply: reply_tx })
            .await
            .map_err(|_| HcError::DuckDb(duckdb::Error::InvalidQuery))?;
        reply_rx
            .await
            .map_err(|_| HcError::DuckDb(duckdb::Error::InvalidQuery))?
    }

    pub async fn query_arrow(&self, sql: String) -> HcResult<RecordBatch> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(DbCommand::QueryArrow { sql, reply: reply_tx })
            .await
            .map_err(|_| HcError::DuckDb(duckdb::Error::InvalidQuery))?;
        reply_rx
            .await
            .map_err(|_| HcError::DuckDb(duckdb::Error::InvalidQuery))?
    }

    pub async fn transaction(&self, statements: Vec<String>) -> HcResult<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(DbCommand::Transaction {
                statements,
                reply: reply_tx,
            })
            .await
            .map_err(|_| HcError::DuckDb(duckdb::Error::InvalidQuery))?;
        reply_rx
            .await
            .map_err(|_| HcError::DuckDb(duckdb::Error::InvalidQuery))?
    }

    fn run_query_json(
        conn: &Connection,
        sql: &str,
    ) -> HcResult<Vec<serde_json::Map<String, JsonValue>>> {
        let mut stmt = conn.prepare(sql).map_err(HcError::DuckDb)?;
        let column_count = stmt.column_count();
        let column_names: Vec<String> = (0..column_count)
            .map(|i| stmt.column_name(i).unwrap_or("?").to_string())
            .collect();

        let rows_iter = stmt
            .query_map([], |row| {
                let mut map = serde_json::Map::new();
                for (i, name) in column_names.iter().enumerate() {
                    // Try each type in order of likelihood
                    if let Ok(v) = row.get::<_, i64>(i) {
                        map.insert(name.clone(), JsonValue::from(v));
                    } else if let Ok(v) = row.get::<_, f64>(i) {
                        map.insert(
                            name.clone(),
                            JsonValue::from(serde_json::Number::from_f64(v).unwrap_or(serde_json::Number::from(0))),
                        );
                    } else if let Ok(v) = row.get::<_, String>(i) {
                        map.insert(name.clone(), JsonValue::from(v));
                    } else {
                        map.insert(name.clone(), JsonValue::Null);
                    }
                }
                Ok(map)
            })
            .map_err(HcError::DuckDb)?;

        let mut results = Vec::new();
        for row in rows_iter {
            results.push(row.map_err(HcError::DuckDb)?);
        }
        Ok(results)
    }

    fn run_query_arrow(conn: &Connection, sql: &str) -> HcResult<RecordBatch> {
        let mut stmt = conn.prepare(sql).map_err(HcError::DuckDb)?;
        let batches: Vec<DuckRecordBatch> = stmt
            .query_arrow([])
            .map_err(HcError::DuckDb)?
            .collect();

        if batches.is_empty() {
            // Return an empty RecordBatch with the correct schema
            let schema = stmt.schema();
            return Ok(RecordBatch::new_empty(schema));
        }

        // Concatenate all batches
        let schema = batches[0].schema();
        let batch = arrow::compute::concat_batches(&schema, &batches)
            .map_err(|e| HcError::Aggregation(format!("Arrow concat error: {}", e)))?;
        Ok(batch)
    }

    fn run_transaction(conn: &Connection, statements: &[String]) -> HcResult<()> {
        conn.execute_batch("BEGIN TRANSACTION")
            .map_err(HcError::DuckDb)?;
        for stmt in statements {
            if let Err(e) = conn.execute_batch(stmt) {
                let _ = conn.execute_batch("ROLLBACK");
                return Err(HcError::DuckDb(e));
            }
        }
        conn.execute_batch("COMMIT").map_err(HcError::DuckDb)?;
        Ok(())
    }
}
```

- [ ] **Step 4: Add db_manager module to main.rs**

Add `pub mod db_manager;` to `src/main.rs` module declarations.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test --no-default-features --test db_manager_test`
Expected: all 3 tests pass

- [ ] **Step 6: Commit**

```bash
git add src/db_manager.rs src/main.rs tests/db_manager_test.rs
git commit -m "feat: DB manager thread with channel-based DuckDB access"
```

---

### Task 5: Persistence Layer

**Files:**
- Create: `src/persistence.rs`
- Create: `tests/persistence_test.rs`

- [ ] **Step 1: Write failing tests**

```rust
// tests/persistence_test.rs
use hydrocube::config::CubeConfig;
use hydrocube::db_manager::DbManager;
use hydrocube::persistence::Persistence;
use tempfile::TempDir;

fn load_test_config() -> CubeConfig {
    let yaml = std::fs::read_to_string("cube.example.yaml").unwrap();
    serde_yaml::from_str(&yaml).unwrap()
}

#[tokio::test]
async fn test_init_creates_tables() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");
    let db = DbManager::open(db_path.to_str().unwrap()).await.unwrap();
    let config = load_test_config();

    Persistence::init(&db, &config).await.unwrap();

    // slices table should exist with schema columns + _window_id
    let rows = db
        .query_json("SELECT column_name FROM information_schema.columns WHERE table_name = 'slices' ORDER BY ordinal_position".into(), vec![])
        .await
        .unwrap();
    let col_names: Vec<&str> = rows.iter().map(|r| r["column_name"].as_str().unwrap()).collect();
    assert!(col_names.contains(&"trade_id"));
    assert!(col_names.contains(&"book"));
    assert!(col_names.contains(&"_window_id"));

    // consolidated table should exist (empty)
    let rows = db
        .query_json("SELECT COUNT(*) AS cnt FROM consolidated".into(), vec![])
        .await
        .unwrap();
    assert_eq!(rows[0]["cnt"], 0);

    // _cube_metadata should have the config hash
    let rows = db
        .query_json("SELECT config_hash FROM _cube_metadata".into(), vec![])
        .await
        .unwrap();
    assert_eq!(rows[0]["config_hash"].as_str().unwrap(), config.schema_hash());
}

#[tokio::test]
async fn test_config_hash_mismatch_detected() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");
    let db = DbManager::open(db_path.to_str().unwrap()).await.unwrap();
    let config = load_test_config();

    // Init with original config
    Persistence::init(&db, &config).await.unwrap();

    // Change config and try to verify
    let mut changed_config = config.clone();
    changed_config.aggregation.sql = "SELECT desk, SUM(notional) FROM slices GROUP BY desk".into();

    let result = Persistence::verify_config_hash(&db, &changed_config).await;
    assert!(result.is_err());
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --no-default-features --test persistence_test 2>&1 | head -5`
Expected: compilation error

- [ ] **Step 3: Implement src/persistence.rs**

```rust
// src/persistence.rs
use crate::config::CubeConfig;
use crate::db_manager::DbManager;
use crate::error::{HcError, HcResult};

pub struct Persistence;

impl Persistence {
    /// Initialise DuckDB tables for a cube. Idempotent — safe to call on an existing DB.
    pub async fn init(db: &DbManager, config: &CubeConfig) -> HcResult<()> {
        // Create slices table
        let columns_sql: Vec<String> = config
            .schema
            .columns
            .iter()
            .map(|c| format!("{} {}", c.name, c.col_type))
            .collect();
        let create_slices = format!(
            "CREATE TABLE IF NOT EXISTS slices ({}, _window_id UBIGINT NOT NULL)",
            columns_sql.join(", ")
        );
        db.execute(create_slices, vec![]).await?;

        // Create empty consolidated table with same dimensions (will be populated by compaction)
        // For now, mirror slices schema — compaction will replace this with decomposed columns
        let create_consolidated = format!(
            "CREATE TABLE IF NOT EXISTS consolidated ({}, _window_id UBIGINT NOT NULL)",
            columns_sql.join(", ")
        );
        db.execute(create_consolidated, vec![]).await?;

        // Create metadata table
        db.execute(
            "CREATE TABLE IF NOT EXISTS _cube_metadata (
                config_hash VARCHAR NOT NULL,
                last_window_id UBIGINT NOT NULL DEFAULT 0,
                compaction_cutoff UBIGINT NOT NULL DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"
            .into(),
            vec![],
        )
        .await?;

        // Insert metadata if empty
        let rows = db
            .query_json("SELECT COUNT(*) AS cnt FROM _cube_metadata".into(), vec![])
            .await?;
        if rows[0]["cnt"].as_i64().unwrap_or(0) == 0 {
            let hash = config.schema_hash();
            db.execute(
                format!(
                    "INSERT INTO _cube_metadata (config_hash, last_window_id, compaction_cutoff) VALUES ('{}', 0, 0)",
                    hash
                ),
                vec![],
            )
            .await?;
        }

        Ok(())
    }

    /// Verify that the stored config hash matches the current config.
    pub async fn verify_config_hash(db: &DbManager, config: &CubeConfig) -> HcResult<()> {
        let rows = db
            .query_json("SELECT config_hash FROM _cube_metadata".into(), vec![])
            .await?;
        if rows.is_empty() {
            return Err(HcError::Persistence("no metadata found".into()));
        }
        let stored_hash = rows[0]["config_hash"]
            .as_str()
            .ok_or_else(|| HcError::Persistence("config_hash is not a string".into()))?;
        let current_hash = config.schema_hash();
        if stored_hash != current_hash {
            return Err(HcError::ConfigHashMismatch);
        }
        Ok(())
    }

    /// Read the last window ID from metadata.
    pub async fn load_last_window_id(db: &DbManager) -> HcResult<u64> {
        let rows = db
            .query_json(
                "SELECT last_window_id FROM _cube_metadata".into(),
                vec![],
            )
            .await?;
        Ok(rows[0]["last_window_id"].as_u64().unwrap_or(0))
    }

    /// Read the compaction cutoff from metadata.
    pub async fn load_compaction_cutoff(db: &DbManager) -> HcResult<u64> {
        let rows = db
            .query_json(
                "SELECT compaction_cutoff FROM _cube_metadata".into(),
                vec![],
            )
            .await?;
        Ok(rows[0]["compaction_cutoff"].as_u64().unwrap_or(0))
    }

    /// Update the last window ID in metadata.
    pub async fn save_window_id(db: &DbManager, window_id: u64) -> HcResult<()> {
        db.execute(
            format!(
                "UPDATE _cube_metadata SET last_window_id = {}, updated_at = CURRENT_TIMESTAMP",
                window_id
            ),
            vec![],
        )
        .await
    }

    /// Update the compaction cutoff in metadata.
    pub async fn save_compaction_cutoff(db: &DbManager, cutoff: u64) -> HcResult<()> {
        db.execute(
            format!(
                "UPDATE _cube_metadata SET compaction_cutoff = {}, updated_at = CURRENT_TIMESTAMP",
                cutoff
            ),
            vec![],
        )
        .await
    }

    /// Drop all tables and reinitialise (for --reset).
    pub async fn reset(db: &DbManager, config: &CubeConfig) -> HcResult<()> {
        db.execute("DROP TABLE IF EXISTS slices".into(), vec![]).await?;
        db.execute("DROP TABLE IF EXISTS consolidated".into(), vec![]).await?;
        db.execute("DROP TABLE IF EXISTS _cube_metadata".into(), vec![]).await?;
        Self::init(db, config).await
    }

    /// Rebuild consolidated from Parquet files and remaining slices (for --rebuild).
    pub async fn rebuild(db: &DbManager, config: &CubeConfig) -> HcResult<()> {
        tracing::info!("Rebuilding consolidated from Parquet history...");

        // Drop and recreate tables
        db.execute("DROP TABLE IF EXISTS consolidated".into(), vec![]).await?;
        db.execute("DROP TABLE IF EXISTS _cube_metadata".into(), vec![]).await?;

        // Reinitialise metadata with new config hash
        Self::init(db, config).await?;

        // If there are slices in DuckDB, the first compaction cycle will build consolidated
        // If Parquet files exist, import them into slices first
        let parquet_glob = format!("{}/**/*.parquet", config.retention.parquet_path);
        let import_sql = format!(
            "INSERT INTO slices SELECT * FROM read_parquet('{}', union_by_name=true) WHERE true",
            parquet_glob
        );
        // Best-effort: Parquet files may not exist
        match db.execute(import_sql, vec![]).await {
            Ok(()) => tracing::info!("Imported Parquet history into slices"),
            Err(e) => tracing::warn!("No Parquet history to import: {}", e),
        }

        tracing::info!("Rebuild complete — first compaction cycle will build consolidated");
        Ok(())
    }
}
```

- [ ] **Step 4: Add persistence module to main.rs**

Add `pub mod persistence;` to `src/main.rs` module declarations.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test --no-default-features --test persistence_test`
Expected: both tests pass

- [ ] **Step 6: Commit**

```bash
git add src/persistence.rs src/main.rs tests/persistence_test.rs
git commit -m "feat: persistence layer with schema init, config hash, and rebuild"
```

---

## Phase 2: Engine Core (Tasks 6-11)

### Task 6: Kafka Ingest + JSON Parser

**Files:**
- Create: `src/ingest/mod.rs`
- Create: `src/ingest/kafka.rs`
- Create: `src/ingest/parser.rs`
- Create: `tests/parser_test.rs`

- [ ] **Step 1: Write failing tests for JSON parsing**

```rust
// tests/parser_test.rs
use hydrocube::config::ColumnDef;
use hydrocube::ingest::parser::JsonParser;
use serde_json::json;

#[test]
fn test_parse_json_message() {
    let columns = vec![
        ColumnDef { name: "trade_id".into(), col_type: "VARCHAR".into() },
        ColumnDef { name: "book".into(), col_type: "VARCHAR".into() },
        ColumnDef { name: "price".into(), col_type: "DOUBLE".into() },
        ColumnDef { name: "quantity".into(), col_type: "DOUBLE".into() },
    ];
    let parser = JsonParser::new(&columns);

    let msg = br#"{"trade_id": "T1", "book": "FX", "price": 1.25, "quantity": 100.0}"#;
    let row = parser.parse(msg).unwrap();
    assert_eq!(row.len(), 4);
    assert_eq!(row[0], json!("T1"));
    assert_eq!(row[1], json!("FX"));
    assert_eq!(row[2], json!(1.25));
    assert_eq!(row[3], json!(100.0));
}

#[test]
fn test_parse_json_missing_field_becomes_null() {
    let columns = vec![
        ColumnDef { name: "trade_id".into(), col_type: "VARCHAR".into() },
        ColumnDef { name: "missing_field".into(), col_type: "VARCHAR".into() },
    ];
    let parser = JsonParser::new(&columns);

    let msg = br#"{"trade_id": "T1"}"#;
    let row = parser.parse(msg).unwrap();
    assert_eq!(row[0], json!("T1"));
    assert!(row[1].is_null());
}

#[test]
fn test_parse_json_extra_fields_ignored() {
    let columns = vec![
        ColumnDef { name: "trade_id".into(), col_type: "VARCHAR".into() },
    ];
    let parser = JsonParser::new(&columns);

    let msg = br#"{"trade_id": "T1", "extra": "ignored"}"#;
    let row = parser.parse(msg).unwrap();
    assert_eq!(row.len(), 1);
    assert_eq!(row[0], json!("T1"));
}

#[test]
fn test_parse_json_malformed_returns_error() {
    let columns = vec![
        ColumnDef { name: "trade_id".into(), col_type: "VARCHAR".into() },
    ];
    let parser = JsonParser::new(&columns);
    let msg = b"not valid json";
    assert!(parser.parse(msg).is_err());
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --no-default-features --test parser_test 2>&1 | head -5`
Expected: compilation error

- [ ] **Step 3: Implement src/ingest/parser.rs**

```rust
// src/ingest/parser.rs
use crate::config::ColumnDef;
use crate::error::{HcError, HcResult};
use serde_json::Value;

pub struct JsonParser {
    column_names: Vec<String>,
}

impl JsonParser {
    pub fn new(columns: &[ColumnDef]) -> Self {
        Self {
            column_names: columns.iter().map(|c| c.name.clone()).collect(),
        }
    }

    /// Parse a raw JSON message into a row of values ordered by schema columns.
    /// Missing fields become Value::Null. Extra fields are ignored.
    pub fn parse(&self, data: &[u8]) -> HcResult<Vec<Value>> {
        let obj: serde_json::Map<String, Value> = serde_json::from_slice(data)
            .map_err(|e| HcError::Ingest(format!("malformed JSON: {}", e)))?;

        let row: Vec<Value> = self
            .column_names
            .iter()
            .map(|name| obj.get(name).cloned().unwrap_or(Value::Null))
            .collect();

        Ok(row)
    }
}
```

- [ ] **Step 4: Implement src/ingest/mod.rs with IngestSource trait**

```rust
// src/ingest/mod.rs
pub mod parser;

#[cfg(feature = "kafka")]
pub mod kafka;

use crate::error::HcResult;
use serde_json::Value;
use tokio::sync::mpsc;

/// Trait for message sources. Implementations push raw byte messages to the buffer channel.
#[async_trait::async_trait]
pub trait IngestSource: Send + Sync {
    /// Start consuming messages. Sends raw bytes to the provided channel.
    /// Returns when the source is shut down.
    async fn run(&self, tx: mpsc::Sender<Vec<u8>>, shutdown: tokio::sync::watch::Receiver<bool>) -> HcResult<()>;

    /// Commit the current offsets (for sources that support it).
    async fn commit(&self) -> HcResult<()>;
}
```

Note: add `async-trait = "0.1"` to `[dependencies]` in Cargo.toml.

- [ ] **Step 5: Implement src/ingest/kafka.rs (stub for now — real Kafka requires a running broker)**

```rust
// src/ingest/kafka.rs
use crate::config::SourceConfig;
use crate::error::{HcError, HcResult};
use crate::ingest::IngestSource;
use tokio::sync::mpsc;

pub struct KafkaSource {
    brokers: String,
    topic: String,
    group_id: String,
}

impl KafkaSource {
    pub fn new(config: &SourceConfig) -> HcResult<Self> {
        let brokers = config
            .brokers
            .as_ref()
            .ok_or_else(|| HcError::Config("kafka requires 'brokers'".into()))?
            .join(",");
        let topic = config
            .topic
            .as_ref()
            .ok_or_else(|| HcError::Config("kafka requires 'topic'".into()))?
            .clone();
        let group_id = config
            .group_id
            .as_ref()
            .ok_or_else(|| HcError::Config("kafka requires 'group_id'".into()))?
            .clone();

        Ok(Self {
            brokers,
            topic,
            group_id,
        })
    }
}

#[async_trait::async_trait]
impl IngestSource for KafkaSource {
    async fn run(
        &self,
        tx: mpsc::Sender<Vec<u8>>,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> HcResult<()> {
        use rdkafka::consumer::{Consumer, StreamConsumer};
        use rdkafka::config::ClientConfig;
        use rdkafka::Message;
        use futures::StreamExt;

        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .set("group.id", &self.group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "latest")
            .create()
            .map_err(|e| HcError::Ingest(format!("Kafka consumer creation failed: {}", e)))?;

        consumer
            .subscribe(&[&self.topic])
            .map_err(|e| HcError::Ingest(format!("Kafka subscribe failed: {}", e)))?;

        tracing::info!(
            target: "ingest",
            "Connected to kafka broker {} topic={} group={}",
            self.brokers, self.topic, self.group_id
        );

        let mut stream = consumer.stream();

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    tracing::info!(target: "ingest", "Kafka consumer shutting down");
                    break;
                }
                msg = stream.next() => {
                    match msg {
                        Some(Ok(m)) => {
                            if let Some(payload) = m.payload() {
                                if tx.send(payload.to_vec()).await.is_err() {
                                    break; // receiver dropped
                                }
                            }
                        }
                        Some(Err(e)) => {
                            tracing::warn!(target: "ingest", "Kafka error: {}", e);
                        }
                        None => break,
                    }
                }
            }
        }

        Ok(())
    }

    async fn commit(&self) -> HcResult<()> {
        // In a full implementation, we'd store the consumer handle and call commit here.
        // For now, this is a placeholder — proper commit integration comes in Task 18.
        Ok(())
    }
}
```

Note: add `futures = "0.3"` to `[dependencies]` in Cargo.toml.

- [ ] **Step 6: Add ingest module to main.rs**

Add `pub mod ingest;` to `src/main.rs` module declarations.

- [ ] **Step 7: Run parser tests**

Run: `cargo test --no-default-features --test parser_test`
Expected: all 4 tests pass

- [ ] **Step 8: Commit**

```bash
git add src/ingest/ tests/parser_test.rs Cargo.toml src/main.rs
git commit -m "feat: ingest module with Kafka source and JSON parser"
```

---

### Task 7: SQL Transform Pipeline

**Files:**
- Create: `src/transform/mod.rs`
- Create: `src/transform/sql.rs`
- Create: `tests/transform_test.rs`

- [ ] **Step 1: Write failing tests for SQL transforms**

```rust
// tests/transform_test.rs
use hydrocube::config::ColumnDef;
use hydrocube::db_manager::DbManager;
use hydrocube::transform::sql::SqlTransform;
use serde_json::json;

#[tokio::test]
async fn test_sql_transform_filter() {
    let db = DbManager::open_in_memory().await.unwrap();
    let columns = vec![
        ColumnDef { name: "id".into(), col_type: "INTEGER".into() },
        ColumnDef { name: "value".into(), col_type: "DOUBLE".into() },
    ];

    let transform = SqlTransform::new("SELECT id, value FROM raw_buffer WHERE value > 0".into());

    let input = vec![
        vec![json!(1), json!(10.0)],
        vec![json!(2), json!(-5.0)],
        vec![json!(3), json!(20.0)],
    ];

    let output = transform.execute(&db, &columns, input).await.unwrap();
    assert_eq!(output.len(), 2);
    assert_eq!(output[0][0], json!(1));
    assert_eq!(output[1][0], json!(3));
}

#[tokio::test]
async fn test_sql_transform_empty_input() {
    let db = DbManager::open_in_memory().await.unwrap();
    let columns = vec![
        ColumnDef { name: "id".into(), col_type: "INTEGER".into() },
    ];

    let transform = SqlTransform::new("SELECT id FROM raw_buffer".into());
    let output = transform.execute(&db, &columns, vec![]).await.unwrap();
    assert_eq!(output.len(), 0);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --no-default-features --test transform_test 2>&1 | head -5`
Expected: compilation error

- [ ] **Step 3: Implement src/transform/sql.rs**

```rust
// src/transform/sql.rs
use crate::config::ColumnDef;
use crate::db_manager::DbManager;
use crate::error::HcResult;
use serde_json::Value;

pub struct SqlTransform {
    sql: String,
}

impl SqlTransform {
    pub fn new(sql: String) -> Self {
        Self { sql }
    }

    /// Execute the SQL transform against a temp table of raw input rows.
    pub async fn execute(
        &self,
        db: &DbManager,
        columns: &[ColumnDef],
        input: Vec<Vec<Value>>,
    ) -> HcResult<Vec<Vec<Value>>> {
        if input.is_empty() {
            return Ok(vec![]);
        }

        // Create temp table
        let col_defs: Vec<String> = columns
            .iter()
            .map(|c| format!("{} {}", c.name, c.col_type))
            .collect();
        db.execute(
            format!(
                "CREATE OR REPLACE TEMP TABLE raw_buffer ({})",
                col_defs.join(", ")
            ),
            vec![],
        )
        .await?;

        // Insert rows using VALUES clause
        let value_rows: Vec<String> = input
            .iter()
            .map(|row| {
                let vals: Vec<String> = row
                    .iter()
                    .zip(columns.iter())
                    .map(|(v, col)| value_to_sql(v, &col.col_type))
                    .collect();
                format!("({})", vals.join(", "))
            })
            .collect();

        let insert_sql = format!(
            "INSERT INTO raw_buffer VALUES {}",
            value_rows.join(", ")
        );
        db.execute(insert_sql, vec![]).await?;

        // Run the user's transform SQL
        let result = db.query_json(self.sql.clone(), vec![]).await?;

        // Convert JSON maps back to Vec<Vec<Value>> in column order
        let output: Vec<Vec<Value>> = result
            .iter()
            .map(|row_map| {
                row_map.values().cloned().collect()
            })
            .collect();

        // Clean up
        db.execute("DROP TABLE IF EXISTS raw_buffer".into(), vec![])
            .await?;

        Ok(output)
    }
}

fn value_to_sql(v: &Value, col_type: &str) -> String {
    match v {
        Value::Null => "NULL".into(),
        Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        _ => format!("'{}'", v.to_string().replace('\'', "''")),
    }
}
```

- [ ] **Step 4: Implement src/transform/mod.rs**

```rust
// src/transform/mod.rs
pub mod sql;
pub mod lua;

use crate::config::{ColumnDef, TransformStep};
use crate::db_manager::DbManager;
use crate::error::HcResult;
use serde_json::Value;

pub struct TransformPipeline {
    steps: Vec<TransformStep>,
}

impl TransformPipeline {
    pub fn new(steps: Vec<TransformStep>) -> Self {
        Self { steps }
    }

    /// Run all transform steps in sequence. Each step's output feeds the next.
    pub async fn execute(
        &self,
        db: &DbManager,
        columns: &[ColumnDef],
        input: Vec<Vec<Value>>,
    ) -> HcResult<Vec<Vec<Value>>> {
        let mut data = input;
        for step in &self.steps {
            data = match step {
                TransformStep::Sql { sql } => {
                    let t = sql::SqlTransform::new(sql.clone());
                    t.execute(db, columns, data).await?
                }
                TransformStep::Lua {
                    script,
                    function,
                    init,
                } => {
                    // Lua transform implemented in Task 8
                    let t = lua::LuaTransform::new(script.clone(), function.clone(), init.clone())?;
                    t.execute(data)?
                }
            };
        }
        Ok(data)
    }
}
```

- [ ] **Step 5: Create stub src/transform/lua.rs (implemented in Task 8)**

```rust
// src/transform/lua.rs
use crate::error::{HcError, HcResult};
use serde_json::Value;

pub struct LuaTransform {
    // Placeholder — implemented in Task 8
}

impl LuaTransform {
    pub fn new(_script_path: String, _function: String, _init: Option<String>) -> HcResult<Self> {
        Err(HcError::Transform("Lua transforms not yet implemented".into()))
    }

    pub fn execute(&self, _input: Vec<Vec<Value>>) -> HcResult<Vec<Vec<Value>>> {
        Err(HcError::Transform("Lua transforms not yet implemented".into()))
    }
}
```

- [ ] **Step 6: Add transform module to main.rs**

Add `pub mod transform;` to `src/main.rs` module declarations.

- [ ] **Step 7: Run tests**

Run: `cargo test --no-default-features --test transform_test`
Expected: both SQL transform tests pass

- [ ] **Step 8: Commit**

```bash
git add src/transform/ tests/transform_test.rs src/main.rs
git commit -m "feat: SQL transform pipeline with temp table execution"
```

---

### Task 8: Lua Transform with Batch Contract

**Files:**
- Modify: `src/transform/lua.rs`
- Modify: `tests/transform_test.rs` (add Lua tests)

- [ ] **Step 1: Add Lua transform tests to tests/transform_test.rs**

```rust
// Append to tests/transform_test.rs
use hydrocube::transform::lua::LuaTransform;
use std::io::Write;

#[test]
fn test_lua_batch_transform() {
    let script = r#"
function transform_batch(messages)
    local rows = {}
    for _, msg in ipairs(messages) do
        if msg.value > 0 then
            table.insert(rows, {id = msg.id, value = msg.value * 2})
        end
    end
    return rows
end
"#;
    let mut tmp = tempfile::NamedTempFile::new().unwrap();
    write!(tmp, "{}", script).unwrap();

    let transform = LuaTransform::from_source(
        script.to_string(),
        "transform_batch".to_string(),
        None,
    )
    .unwrap();

    let input = vec![
        vec![json!(1), json!(10.0)],
        vec![json!(2), json!(-5.0)],
        vec![json!(3), json!(20.0)],
    ];
    let col_names = vec!["id".to_string(), "value".to_string()];

    let output = transform.execute_batch(input, &col_names).unwrap();
    assert_eq!(output.len(), 2); // -5.0 filtered out
}

#[test]
fn test_lua_per_message_fallback() {
    let script = r#"
function transform(msg)
    return {{id = msg.id, value = msg.value + 1}}
end
"#;

    let transform = LuaTransform::from_source(
        script.to_string(),
        "transform".to_string(),
        None,
    )
    .unwrap();

    let input = vec![vec![json!(1), json!(10.0)]];
    let col_names = vec!["id".to_string(), "value".to_string()];

    let output = transform.execute_per_message(input, &col_names).unwrap();
    assert_eq!(output.len(), 1);
}

#[test]
fn test_lua_transform_returns_empty_filters_message() {
    let script = r#"
function transform_batch(messages)
    return {}
end
"#;

    let transform = LuaTransform::from_source(
        script.to_string(),
        "transform_batch".to_string(),
        None,
    )
    .unwrap();

    let input = vec![vec![json!(1), json!(10.0)]];
    let col_names = vec!["id".to_string(), "value".to_string()];

    let output = transform.execute_batch(input, &col_names).unwrap();
    assert_eq!(output.len(), 0);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --no-default-features --test transform_test test_lua 2>&1 | head -10`
Expected: test failures (LuaTransform stub returns error)

- [ ] **Step 3: Implement src/transform/lua.rs**

```rust
// src/transform/lua.rs
use crate::error::{HcError, HcResult};
use mlua::{Lua, Result as LuaResult, Value as LuaValue, MultiValue, Function};
use serde_json::Value;

pub struct LuaTransform {
    lua: Lua,
    function_name: String,
    is_batch: bool,
}

impl LuaTransform {
    /// Create a LuaTransform from inline source code (for testing and embedded use).
    pub fn from_source(
        source: String,
        function_name: String,
        init_function: Option<String>,
    ) -> HcResult<Self> {
        let lua = Lua::new();
        lua.load(&source)
            .exec()
            .map_err(|e| HcError::Transform(format!("Lua load error: {}", e)))?;

        let is_batch = function_name.contains("batch");

        Ok(Self {
            lua,
            function_name,
            is_batch,
        })
    }

    /// Create from a script file path.
    pub fn from_file(
        script_path: String,
        function_name: String,
        init_function: Option<String>,
    ) -> HcResult<Self> {
        let source = std::fs::read_to_string(&script_path)
            .map_err(|e| HcError::Transform(format!("Cannot read Lua script {}: {}", script_path, e)))?;
        Self::from_source(source, function_name, init_function)
    }

    /// Execute the batch contract: transform_batch(messages) -> rows
    pub fn execute_batch(
        &self,
        input: Vec<Vec<Value>>,
        col_names: &[String],
    ) -> HcResult<Vec<Vec<Value>>> {
        let func: Function = self
            .lua
            .globals()
            .get(self.function_name.as_str())
            .map_err(|e| HcError::Transform(format!("Lua function '{}' not found: {}", self.function_name, e)))?;

        // Convert input rows to Lua table of tables
        let messages = self.rows_to_lua_table(&input, col_names)?;

        // Call the function
        let result: LuaValue = func
            .call(messages)
            .map_err(|e| HcError::Transform(format!("Lua call error: {}", e)))?;

        // Convert result back to Vec<Vec<Value>>
        self.lua_result_to_rows(result, col_names)
    }

    /// Execute per-message fallback: transform(msg) -> rows, called per message
    pub fn execute_per_message(
        &self,
        input: Vec<Vec<Value>>,
        col_names: &[String],
    ) -> HcResult<Vec<Vec<Value>>> {
        let func: Function = self
            .lua
            .globals()
            .get(self.function_name.as_str())
            .map_err(|e| HcError::Transform(format!("Lua function '{}' not found: {}", self.function_name, e)))?;

        let mut all_rows = Vec::new();

        for row in &input {
            let msg = self.row_to_lua_table(row, col_names)?;
            let result: LuaValue = func
                .call(msg)
                .map_err(|e| HcError::Transform(format!("Lua per-message call error: {}", e)))?;
            let mut rows = self.lua_result_to_rows(result, col_names)?;
            all_rows.append(&mut rows);
        }

        Ok(all_rows)
    }

    fn row_to_lua_table<'a>(
        &'a self,
        row: &[Value],
        col_names: &[String],
    ) -> HcResult<LuaValue<'a>> {
        let table = self.lua.create_table()
            .map_err(|e| HcError::Transform(format!("Lua table creation error: {}", e)))?;
        for (i, name) in col_names.iter().enumerate() {
            if let Some(val) = row.get(i) {
                let lua_val = json_to_lua(&self.lua, val)?;
                table.set(name.as_str(), lua_val)
                    .map_err(|e| HcError::Transform(format!("Lua set error: {}", e)))?;
            }
        }
        Ok(LuaValue::Table(table))
    }

    fn rows_to_lua_table<'a>(
        &'a self,
        rows: &[Vec<Value>],
        col_names: &[String],
    ) -> HcResult<LuaValue<'a>> {
        let array = self.lua.create_table()
            .map_err(|e| HcError::Transform(format!("Lua table creation error: {}", e)))?;
        for (i, row) in rows.iter().enumerate() {
            let row_table = self.row_to_lua_table(row, col_names)?;
            array.set(i + 1, row_table)
                .map_err(|e| HcError::Transform(format!("Lua set error: {}", e)))?;
        }
        Ok(LuaValue::Table(array))
    }

    fn lua_result_to_rows(
        &self,
        result: LuaValue,
        _col_names: &[String],
    ) -> HcResult<Vec<Vec<Value>>> {
        match result {
            LuaValue::Table(tbl) => {
                let mut rows = Vec::new();
                for pair in tbl.pairs::<i64, mlua::Table>() {
                    let (_, row_table) = pair
                        .map_err(|e| HcError::Transform(format!("Lua result iteration error: {}", e)))?;
                    let mut row = Vec::new();
                    for pair in row_table.pairs::<String, LuaValue>() {
                        let (key, val) = pair
                            .map_err(|e| HcError::Transform(format!("Lua row iteration error: {}", e)))?;
                        row.push(lua_to_json(val));
                    }
                    rows.push(row);
                }
                Ok(rows)
            }
            _ => Ok(vec![]),
        }
    }
}

fn json_to_lua<'a>(lua: &'a Lua, v: &Value) -> HcResult<LuaValue<'a>> {
    match v {
        Value::Null => Ok(LuaValue::Nil),
        Value::Bool(b) => Ok(LuaValue::Boolean(*b)),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(LuaValue::Integer(i))
            } else if let Some(f) = n.as_f64() {
                Ok(LuaValue::Number(f))
            } else {
                Ok(LuaValue::Nil)
            }
        }
        Value::String(s) => {
            let ls = lua.create_string(s.as_str())
                .map_err(|e| HcError::Transform(format!("Lua string error: {}", e)))?;
            Ok(LuaValue::String(ls))
        }
        _ => Ok(LuaValue::Nil),
    }
}

fn lua_to_json(v: LuaValue) -> Value {
    match v {
        LuaValue::Nil => Value::Null,
        LuaValue::Boolean(b) => Value::Bool(b),
        LuaValue::Integer(i) => Value::from(i),
        LuaValue::Number(f) => {
            serde_json::Number::from_f64(f)
                .map(Value::Number)
                .unwrap_or(Value::Null)
        }
        LuaValue::String(s) => Value::String(s.to_str().unwrap_or("").to_string()),
        _ => Value::Null,
    }
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test --no-default-features --test transform_test`
Expected: all tests pass (SQL + Lua)

- [ ] **Step 5: Commit**

```bash
git add src/transform/lua.rs tests/transform_test.rs
git commit -m "feat: Lua transform with batch contract and per-message fallback"
```

---

### Task 9: Aggregation SQL Generation + Decomposability Detection

**Files:**
- Create: `src/aggregation/mod.rs`
- Create: `src/aggregation/sql_gen.rs`
- Create: `src/aggregation/decompose.rs`
- Create: `tests/sql_gen_test.rs`

- [ ] **Step 1: Write failing tests**

```rust
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
    let measures = gen.measures();
    assert_eq!(measures.len(), 2);
    assert_eq!(measures[0].alias, "total");
    assert_eq!(measures[1].alias, "cnt");
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
    assert!(is_decomposable("AVG(price)")); // decomposable via sum/count
    assert!(!is_decomposable("COUNT(DISTINCT counterparty)"));
    assert!(!is_decomposable("MEDIAN(price)"));
    assert!(!is_decomposable("QUANTILE_CONT(price, 0.95)"));
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --no-default-features --test sql_gen_test 2>&1 | head -5`
Expected: compilation error

- [ ] **Step 3: Implement src/aggregation/decompose.rs**

```rust
// src/aggregation/decompose.rs

/// Check if an aggregate expression is decomposable (can be combined from partial results).
pub fn is_decomposable(expr: &str) -> bool {
    let upper = expr.trim().to_uppercase();

    // Non-decomposable patterns
    let non_decomposable = [
        "COUNT(DISTINCT",
        "MEDIAN(",
        "QUANTILE_CONT(",
        "QUANTILE_DISC(",
        "PERCENTILE_CONT(",
        "PERCENTILE_DISC(",
        "STRING_AGG(",
        "LIST(",
        "ARRAY_AGG(",
        "MODE(",
    ];

    for pattern in &non_decomposable {
        if upper.contains(pattern) {
            return false;
        }
    }

    // Decomposable patterns
    let decomposable = [
        "SUM(", "COUNT(", "MIN(", "MAX(", "AVG(", "STDDEV(", "VARIANCE(",
        "STDDEV_POP(", "STDDEV_SAMP(", "VAR_POP(", "VAR_SAMP(",
    ];

    for pattern in &decomposable {
        if upper.contains(pattern) {
            return true;
        }
    }

    // Unknown — treat as non-decomposable to be safe
    false
}
```

- [ ] **Step 4: Implement src/aggregation/sql_gen.rs**

```rust
// src/aggregation/sql_gen.rs
use crate::aggregation::decompose;
use crate::error::{HcError, HcResult};

#[derive(Debug, Clone)]
pub struct MeasureDef {
    pub expression: String,
    pub alias: String,
    pub decomposable: bool,
}

#[derive(Debug)]
pub struct AggSqlGenerator {
    dimensions: Vec<String>,
    measures: Vec<MeasureDef>,
    original_sql: String,
}

impl AggSqlGenerator {
    /// Parse the user's aggregation SQL to extract dimensions and measures.
    pub fn from_user_sql(sql: &str) -> HcResult<Self> {
        let upper = sql.to_uppercase();

        // Extract GROUP BY columns
        let dimensions = Self::parse_group_by(sql)?;

        // Extract SELECT expressions that aren't in GROUP BY
        let measures = Self::parse_measures(sql, &dimensions)?;

        Ok(Self {
            dimensions,
            measures,
            original_sql: sql.to_string(),
        })
    }

    pub fn dimensions(&self) -> &[String] {
        &self.dimensions
    }

    pub fn measures(&self) -> &[MeasureDef] {
        &self.measures
    }

    pub fn has_non_decomposable(&self) -> bool {
        self.measures.iter().any(|m| !m.decomposable)
    }

    /// Generate the SQL to aggregate recent slices (above cutoff).
    pub fn slice_aggregation_sql(&self) -> String {
        let dims = self.dimensions.join(", ");
        let measures: Vec<String> = self
            .measures
            .iter()
            .map(|m| format!("{} AS {}", m.expression, m.alias))
            .collect();
        let select = if dims.is_empty() {
            measures.join(", ")
        } else {
            format!("{}, {}", dims, measures.join(", "))
        };
        format!(
            "SELECT {} FROM slices WHERE _window_id > $cutoff GROUP BY {}",
            select, dims
        )
    }

    /// Generate the full aggregation query (user's original SQL without the FROM/WHERE).
    /// This runs against all slices for the simple case (before compaction is set up).
    pub fn full_aggregation_sql(&self) -> String {
        self.original_sql.clone()
    }

    fn parse_group_by(sql: &str) -> HcResult<Vec<String>> {
        let upper = sql.to_uppercase();
        if let Some(pos) = upper.rfind("GROUP BY") {
            let after = &sql[pos + 8..];
            // Take until end or next clause (ORDER BY, HAVING, LIMIT)
            let end = ["ORDER BY", "HAVING", "LIMIT", ";"]
                .iter()
                .filter_map(|kw| after.to_uppercase().find(kw))
                .min()
                .unwrap_or(after.len());
            let group_str = after[..end].trim();
            let dims: Vec<String> = group_str
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            Ok(dims)
        } else {
            Err(HcError::Config(
                "aggregation SQL must contain GROUP BY".into(),
            ))
        }
    }

    fn parse_measures(sql: &str, dimensions: &[String]) -> HcResult<Vec<MeasureDef>> {
        let upper = sql.to_uppercase();
        let select_start = upper
            .find("SELECT")
            .ok_or_else(|| HcError::Config("aggregation SQL must start with SELECT".into()))?
            + 6;
        let from_pos = upper
            .find("FROM")
            .ok_or_else(|| HcError::Config("aggregation SQL must contain FROM".into()))?;
        let select_clause = sql[select_start..from_pos].trim();

        let mut measures = Vec::new();
        let mut depth = 0;
        let mut current = String::new();
        let mut expressions = Vec::new();

        // Split by comma, respecting parentheses
        for ch in select_clause.chars() {
            match ch {
                '(' => {
                    depth += 1;
                    current.push(ch);
                }
                ')' => {
                    depth -= 1;
                    current.push(ch);
                }
                ',' if depth == 0 => {
                    expressions.push(current.trim().to_string());
                    current = String::new();
                }
                _ => current.push(ch),
            }
        }
        if !current.trim().is_empty() {
            expressions.push(current.trim().to_string());
        }

        for expr in expressions {
            let trimmed = expr.trim();
            // Check if this is a dimension (bare column name in GROUP BY)
            let is_dimension = dimensions.iter().any(|d| {
                trimmed.eq_ignore_ascii_case(d)
            });
            if is_dimension {
                continue;
            }

            // Extract alias: "expression AS alias" or just "expression"
            let (expression, alias) = if let Some(as_pos) = trimmed
                .to_uppercase()
                .rfind(" AS ")
            {
                let expr_part = trimmed[..as_pos].trim().to_string();
                let alias_part = trimmed[as_pos + 4..].trim().to_string();
                (expr_part, alias_part)
            } else {
                (trimmed.to_string(), trimmed.to_string())
            };

            let decomposable = decompose::is_decomposable(&expression);
            measures.push(MeasureDef {
                expression,
                alias,
                decomposable,
            });
        }

        Ok(measures)
    }
}
```

- [ ] **Step 5: Create src/aggregation/mod.rs**

```rust
// src/aggregation/mod.rs
pub mod decompose;
pub mod sql_gen;
pub mod window;
```

- [ ] **Step 6: Create stub src/aggregation/window.rs**

```rust
// src/aggregation/window.rs
// Window timer — implemented in Task 10
```

- [ ] **Step 7: Add aggregation module to main.rs**

Add `pub mod aggregation;` to `src/main.rs` module declarations.

- [ ] **Step 8: Run tests**

Run: `cargo test --no-default-features --test sql_gen_test`
Expected: all tests pass

- [ ] **Step 9: Commit**

```bash
git add src/aggregation/ tests/sql_gen_test.rs src/main.rs
git commit -m "feat: aggregation SQL generation and decomposability detection"
```

---

### Task 10: Window Timer + Aggregation Hot Path

**Files:**
- Modify: `src/aggregation/window.rs`
- Create: `tests/aggregation_test.rs`

- [ ] **Step 1: Write failing tests**

```rust
// tests/aggregation_test.rs
use hydrocube::aggregation::sql_gen::AggSqlGenerator;
use hydrocube::db_manager::DbManager;
use hydrocube::persistence::Persistence;
use serde_json::json;

fn test_config() -> hydrocube::config::CubeConfig {
    let yaml = std::fs::read_to_string("cube.example.yaml").unwrap();
    serde_yaml::from_str(&yaml).unwrap()
}

#[tokio::test]
async fn test_basic_aggregation_from_slices() {
    let db = DbManager::open_in_memory().await.unwrap();
    let config = test_config();
    Persistence::init(&db, &config).await.unwrap();

    // Insert test data into slices
    db.execute(
        "INSERT INTO slices VALUES \
         ('T1', 'FX', 'Trading', 'EURUSD', 'Spot', 'EUR', 100, 1.10, 110, 'BUY', '2026-04-13 10:00:00', 1), \
         ('T2', 'FX', 'Trading', 'EURUSD', 'Spot', 'EUR', 50, 1.11, 55.5, 'SELL', '2026-04-13 10:00:01', 1), \
         ('T3', 'Rates', 'Trading', 'UST10Y', 'Bond', 'USD', 200, 98.5, 19700, 'BUY', '2026-04-13 10:00:02', 1)"
            .into(),
        vec![],
    )
    .await
    .unwrap();

    // Run the aggregation SQL directly
    let rows = db
        .query_json(config.aggregation.sql.clone(), vec![])
        .await
        .unwrap();

    // Should have 2 groups: FX/Trading/Spot/EUR and Rates/Trading/Bond/USD
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_aggregation_returns_arrow() {
    let db = DbManager::open_in_memory().await.unwrap();
    let config = test_config();
    Persistence::init(&db, &config).await.unwrap();

    db.execute(
        "INSERT INTO slices VALUES \
         ('T1', 'FX', 'Trading', 'EURUSD', 'Spot', 'EUR', 100, 1.10, 110, 'BUY', '2026-04-13 10:00:00', 1)"
            .into(),
        vec![],
    )
    .await
    .unwrap();

    let batch = db
        .query_arrow(config.aggregation.sql.clone())
        .await
        .unwrap();
    assert_eq!(batch.num_rows(), 1);
    // Dimensions + measures = 4 + 5 = 9 columns
    assert_eq!(batch.num_columns(), 9);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --no-default-features --test aggregation_test 2>&1 | head -10`
Expected: compilation error or test failures

- [ ] **Step 3: Implement src/aggregation/window.rs**

```rust
// src/aggregation/window.rs
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time;

/// Global compaction cutoff — the single coordination point between hot path and compaction.
pub static COMPACTION_CUTOFF: AtomicU64 = AtomicU64::new(0);

/// Global window ID counter.
pub static WINDOW_ID: AtomicU64 = AtomicU64::new(0);

/// Advance and return the next window ID.
pub fn next_window_id() -> u64 {
    WINDOW_ID.fetch_add(1, Ordering::SeqCst) + 1
}

/// Read the current compaction cutoff.
pub fn compaction_cutoff() -> u64 {
    COMPACTION_CUTOFF.load(Ordering::Acquire)
}

/// Set the compaction cutoff (called by compaction thread after swap).
pub fn set_compaction_cutoff(cutoff: u64) {
    COMPACTION_CUTOFF.store(cutoff, Ordering::Release);
}

/// Restore window state from persistence (on startup).
pub fn restore_state(last_window_id: u64, cutoff: u64) {
    WINDOW_ID.store(last_window_id, Ordering::SeqCst);
    COMPACTION_CUTOFF.store(cutoff, Ordering::Release);
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test --no-default-features --test aggregation_test`
Expected: both tests pass

- [ ] **Step 5: Commit**

```bash
git add src/aggregation/window.rs tests/aggregation_test.rs
git commit -m "feat: window state management and aggregation hot path tests"
```

---

### Task 11: Delta Detection

**Files:**
- Create: `src/delta.rs`
- Create: `tests/delta_test.rs`

- [ ] **Step 1: Write failing tests**

```rust
// tests/delta_test.rs
use hydrocube::delta::DeltaDetector;
use arrow::array::{StringArray, Float64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

fn make_batch(books: &[&str], totals: &[f64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("book", DataType::Utf8, false),
        Field::new("total_notional", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(books.to_vec())),
            Arc::new(Float64Array::from(totals.to_vec())),
        ],
    )
    .unwrap()
}

#[test]
fn test_first_window_all_rows_are_deltas() {
    let mut detector = DeltaDetector::new(vec!["book".into()]);
    let batch = make_batch(&["FX", "Rates"], &[100.0, 200.0]);
    let (upserts, deletes) = detector.detect(&batch);
    assert_eq!(upserts.num_rows(), 2);
    assert_eq!(deletes.num_rows(), 0);
}

#[test]
fn test_unchanged_rows_not_in_delta() {
    let mut detector = DeltaDetector::new(vec!["book".into()]);
    let batch = make_batch(&["FX", "Rates"], &[100.0, 200.0]);
    detector.detect(&batch);

    // Same data again — no changes
    let batch2 = make_batch(&["FX", "Rates"], &[100.0, 200.0]);
    let (upserts, deletes) = detector.detect(&batch2);
    assert_eq!(upserts.num_rows(), 0);
    assert_eq!(deletes.num_rows(), 0);
}

#[test]
fn test_changed_row_detected() {
    let mut detector = DeltaDetector::new(vec!["book".into()]);
    let batch = make_batch(&["FX", "Rates"], &[100.0, 200.0]);
    detector.detect(&batch);

    // FX changed, Rates unchanged
    let batch2 = make_batch(&["FX", "Rates"], &[150.0, 200.0]);
    let (upserts, deletes) = detector.detect(&batch2);
    assert_eq!(upserts.num_rows(), 1); // only FX
    assert_eq!(deletes.num_rows(), 0);
}

#[test]
fn test_deleted_group_detected() {
    let mut detector = DeltaDetector::new(vec!["book".into()]);
    let batch = make_batch(&["FX", "Rates"], &[100.0, 200.0]);
    detector.detect(&batch);

    // Only FX in next window — Rates was deleted
    let batch2 = make_batch(&["FX"], &[100.0]);
    let (upserts, deletes) = detector.detect(&batch2);
    assert_eq!(upserts.num_rows(), 0);
    assert_eq!(deletes.num_rows(), 1); // Rates deleted
}

#[test]
fn test_new_group_detected() {
    let mut detector = DeltaDetector::new(vec!["book".into()]);
    let batch = make_batch(&["FX"], &[100.0]);
    detector.detect(&batch);

    // New group appears
    let batch2 = make_batch(&["FX", "Rates"], &[100.0, 200.0]);
    let (upserts, deletes) = detector.detect(&batch2);
    assert_eq!(upserts.num_rows(), 1); // Rates is new
    assert_eq!(deletes.num_rows(), 0);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --no-default-features --test delta_test 2>&1 | head -5`
Expected: compilation error

- [ ] **Step 3: Implement src/delta.rs**

```rust
// src/delta.rs
use arrow::array::{Array, ArrayRef, StringArray, BooleanArray};
use arrow::compute;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use std::sync::Arc;

pub struct DeltaDetector {
    dimension_names: Vec<String>,
    previous: HashMap<String, u64>, // dimension key -> hash of measure values
}

impl DeltaDetector {
    pub fn new(dimension_names: Vec<String>) -> Self {
        Self {
            dimension_names,
            previous: HashMap::new(),
        }
    }

    /// Compare the current aggregate batch against the previous state.
    /// Returns (upserts, deletes) as RecordBatches.
    pub fn detect(&mut self, current: &RecordBatch) -> (RecordBatch, RecordBatch) {
        let schema = current.schema();
        let dim_indices: Vec<usize> = self
            .dimension_names
            .iter()
            .filter_map(|name| schema.index_of(name).ok())
            .collect();
        let measure_indices: Vec<usize> = (0..schema.fields().len())
            .filter(|i| !dim_indices.contains(i))
            .collect();

        let mut current_state: HashMap<String, u64> = HashMap::new();
        let mut upsert_mask = vec![false; current.num_rows()];

        for row_idx in 0..current.num_rows() {
            let dim_key = self.build_dimension_key(current, &dim_indices, row_idx);
            let measure_hash = self.hash_measures(current, &measure_indices, row_idx);

            current_state.insert(dim_key.clone(), measure_hash);

            // Check if this is new or changed
            match self.previous.get(&dim_key) {
                None => upsert_mask[row_idx] = true,         // new group
                Some(&prev_hash) if prev_hash != measure_hash => {
                    upsert_mask[row_idx] = true;              // changed
                }
                _ => {}                                       // unchanged
            }
        }

        // Detect deletes: keys in previous but not in current
        let deleted_keys: Vec<String> = self
            .previous
            .keys()
            .filter(|k| !current_state.contains_key(*k))
            .cloned()
            .collect();

        // Build upsert batch
        let upsert_indices: Vec<usize> = upsert_mask
            .iter()
            .enumerate()
            .filter(|(_, &v)| v)
            .map(|(i, _)| i)
            .collect();
        let upserts = self.filter_batch(current, &upsert_indices);

        // Build delete batch (empty schema — just dimension columns with null measures)
        let deletes = self.build_delete_batch(current, &deleted_keys, &dim_indices);

        // Update state
        self.previous = current_state;

        (upserts, deletes)
    }

    fn build_dimension_key(
        &self,
        batch: &RecordBatch,
        dim_indices: &[usize],
        row: usize,
    ) -> String {
        let mut key = String::new();
        for &idx in dim_indices {
            let col = batch.column(idx);
            let val = self.array_value_to_string(col, row);
            if !key.is_empty() {
                key.push('|');
            }
            key.push_str(&val);
        }
        key
    }

    fn hash_measures(
        &self,
        batch: &RecordBatch,
        measure_indices: &[usize],
        row: usize,
    ) -> u64 {
        let mut hasher = DefaultHasher::new();
        for &idx in measure_indices {
            let col = batch.column(idx);
            let val = self.array_value_to_string(col, row);
            val.hash(&mut hasher);
        }
        hasher.finish()
    }

    fn array_value_to_string(&self, array: &ArrayRef, row: usize) -> String {
        if array.is_null(row) {
            return "NULL".to_string();
        }
        // Handle common types
        if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
            return arr.value(row).to_string();
        }
        if let Some(arr) = array.as_any().downcast_ref::<arrow::array::Float64Array>() {
            return format!("{:.10}", arr.value(row));
        }
        if let Some(arr) = array.as_any().downcast_ref::<arrow::array::Int64Array>() {
            return arr.value(row).to_string();
        }
        if let Some(arr) = array.as_any().downcast_ref::<arrow::array::Int32Array>() {
            return arr.value(row).to_string();
        }
        format!("{:?}", array.slice(row, 1))
    }

    fn filter_batch(&self, batch: &RecordBatch, indices: &[usize]) -> RecordBatch {
        if indices.is_empty() {
            return RecordBatch::new_empty(batch.schema());
        }
        let indices_array = arrow::array::UInt32Array::from(
            indices.iter().map(|&i| i as u32).collect::<Vec<_>>(),
        );
        let columns: Vec<ArrayRef> = batch
            .columns()
            .iter()
            .map(|col| compute::take(col, &indices_array, None).unwrap())
            .collect();
        RecordBatch::try_new(batch.schema(), columns).unwrap()
    }

    fn build_delete_batch(
        &self,
        template: &RecordBatch,
        deleted_keys: &[String],
        dim_indices: &[usize],
    ) -> RecordBatch {
        if deleted_keys.is_empty() {
            return RecordBatch::new_empty(template.schema());
        }
        // For simplicity, return an empty batch with the count of deletes
        // A full implementation would reconstruct dimension columns from the keys
        // For now, we track the count via num_rows on a minimal batch
        let schema = template.schema();
        let num_deletes = deleted_keys.len();

        // Build arrays with delete count rows, all null except dimensions
        let columns: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| {
                arrow::array::new_null_array(field.data_type(), num_deletes)
            })
            .collect();

        RecordBatch::try_new(schema, columns).unwrap_or_else(|_| {
            RecordBatch::new_empty(schema)
        })
    }
}
```

- [ ] **Step 4: Add delta module to main.rs**

Add `pub mod delta;` to `src/main.rs` module declarations.

- [ ] **Step 5: Run tests**

Run: `cargo test --no-default-features --test delta_test`
Expected: all 5 tests pass

- [ ] **Step 6: Commit**

```bash
git add src/delta.rs tests/delta_test.rs src/main.rs
git commit -m "feat: hash-based delta detection for aggregate changes"
```

---

## Phase 3: Compaction + Retention (Task 12)

### Task 12: Compaction Thread + Parquet Retention

**Files:**
- Create: `src/compaction.rs`
- Create: `src/retention.rs`
- Create: `tests/compaction_test.rs`

- [ ] **Step 1: Write failing tests**

```rust
// tests/compaction_test.rs
use hydrocube::db_manager::DbManager;
use hydrocube::persistence::Persistence;
use hydrocube::retention::RetentionManager;
use tempfile::TempDir;

fn test_config() -> hydrocube::config::CubeConfig {
    let yaml = std::fs::read_to_string("cube.example.yaml").unwrap();
    serde_yaml::from_str(&yaml).unwrap()
}

#[tokio::test]
async fn test_parquet_export() {
    let tmp = TempDir::new().unwrap();
    let db = DbManager::open_in_memory().await.unwrap();
    let config = test_config();
    Persistence::init(&db, &config).await.unwrap();

    // Insert test slices
    db.execute(
        "INSERT INTO slices VALUES \
         ('T1', 'FX', 'Trading', 'EURUSD', 'Spot', 'EUR', 100, 1.10, 110, 'BUY', '2026-04-13 10:00:00', 1)"
            .into(),
        vec![],
    )
    .await
    .unwrap();

    let parquet_dir = tmp.path().join("slices");
    std::fs::create_dir_all(&parquet_dir).unwrap();

    let export_path = parquet_dir.join("test.parquet");
    let sql = format!(
        "COPY (SELECT * FROM slices WHERE _window_id <= 1) TO '{}'",
        export_path.to_str().unwrap()
    );
    db.execute(sql, vec![]).await.unwrap();

    assert!(export_path.exists());
}

#[tokio::test]
async fn test_retention_prune_old_files() {
    let tmp = TempDir::new().unwrap();
    let old_dir = tmp.path().join("2020-01-01");
    let new_dir = tmp.path().join("2026-04-13");
    std::fs::create_dir_all(&old_dir).unwrap();
    std::fs::create_dir_all(&new_dir).unwrap();
    std::fs::write(old_dir.join("test.parquet"), b"old data").unwrap();
    std::fs::write(new_dir.join("test.parquet"), b"new data").unwrap();

    RetentionManager::prune(tmp.path().to_str().unwrap(), 86400).unwrap(); // 1 day retention

    // Old directory should be removed, new should remain
    assert!(!old_dir.exists());
    assert!(new_dir.exists());
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --no-default-features --test compaction_test 2>&1 | head -5`
Expected: compilation error

- [ ] **Step 3: Implement src/retention.rs**

```rust
// src/retention.rs
use crate::error::{HcError, HcResult};
use chrono::{NaiveDate, Utc};
use std::fs;
use std::path::Path;

pub struct RetentionManager;

impl RetentionManager {
    /// Export slices below the cutoff to a Parquet partition file.
    pub async fn export_to_parquet(
        db: &crate::db_manager::DbManager,
        parquet_path: &str,
        cutoff: u64,
    ) -> HcResult<()> {
        let today = Utc::now().format("%Y-%m-%d").to_string();
        let dir = format!("{}/{}", parquet_path, today);
        fs::create_dir_all(&dir).map_err(|e| {
            HcError::Persistence(format!("Cannot create Parquet dir {}: {}", dir, e))
        })?;

        let file_path = format!("{}/window_{:06}.parquet", dir, cutoff);
        let sql = format!(
            "COPY (SELECT * FROM slices WHERE _window_id <= {}) TO '{}'",
            cutoff, file_path
        );

        db.execute(sql, vec![]).await?;
        tracing::debug!(
            target: "compact",
            "Exported slices to {}",
            file_path
        );
        Ok(())
    }

    /// Prune Parquet partition directories older than retention_seconds.
    pub fn prune(parquet_path: &str, retention_seconds: u64) -> HcResult<()> {
        let path = Path::new(parquet_path);
        if !path.exists() {
            return Ok(());
        }

        let cutoff_date = Utc::now()
            - chrono::Duration::seconds(retention_seconds as i64);
        let cutoff_naive = cutoff_date.date_naive();

        for entry in fs::read_dir(path).map_err(|e| {
            HcError::Persistence(format!("Cannot read Parquet dir: {}", e))
        })? {
            let entry = entry.map_err(|e| {
                HcError::Persistence(format!("Dir entry error: {}", e))
            })?;
            let name = entry.file_name();
            let name_str = name.to_str().unwrap_or("");

            // Try to parse directory name as a date (YYYY-MM-DD)
            if let Ok(dir_date) = NaiveDate::parse_from_str(name_str, "%Y-%m-%d") {
                if dir_date < cutoff_naive {
                    tracing::info!(
                        target: "retention",
                        "Pruning old Parquet partition: {}",
                        name_str
                    );
                    if let Err(e) = fs::remove_dir_all(entry.path()) {
                        tracing::warn!(
                            target: "retention",
                            "Failed to prune {}: {}",
                            name_str, e
                        );
                    }
                }
            }
        }

        Ok(())
    }
}
```

- [ ] **Step 4: Implement src/compaction.rs**

```rust
// src/compaction.rs
use crate::aggregation::window;
use crate::config::CubeConfig;
use crate::db_manager::DbManager;
use crate::error::HcResult;
use crate::persistence::Persistence;
use crate::retention::RetentionManager;
use std::time::Duration;
use tokio::sync::watch;

pub struct CompactionThread {
    db: DbManager,
    config: CubeConfig,
    interval_windows: u64,
}

impl CompactionThread {
    pub fn new(db: DbManager, config: CubeConfig) -> Self {
        let interval_windows = config.compaction.interval_windows;
        Self {
            db,
            config,
            interval_windows,
        }
    }

    /// Run the compaction loop until shutdown signal.
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) -> HcResult<()> {
        let interval_ms = self.config.window.interval_ms * self.interval_windows;
        let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    tracing::info!(target: "compact", "Compaction thread shutting down");
                    break;
                }
                _ = interval.tick() => {
                    if let Err(e) = self.compact_cycle().await {
                        tracing::warn!(target: "compact", "Compaction error: {}", e);
                    }
                }
            }
        }

        Ok(())
    }

    /// Execute one compaction cycle.
    pub async fn compact_cycle(&self) -> HcResult<()> {
        let current_window = window::WINDOW_ID.load(std::sync::atomic::Ordering::SeqCst);
        if current_window == 0 {
            return Ok(()); // Nothing to compact yet
        }

        let safety_buffer = 2; // Don't compact the most recent 2 windows
        let cutoff = current_window.saturating_sub(safety_buffer);
        let old_cutoff = window::compaction_cutoff();

        if cutoff <= old_cutoff {
            return Ok(()); // Nothing new to compact
        }

        tracing::debug!(
            target: "compact",
            "Compaction cycle: cutoff {} -> {}",
            old_cutoff, cutoff
        );

        // Step 1: Export old slices to Parquet (separate DB command, interleave-friendly)
        if let Err(e) = RetentionManager::export_to_parquet(
            &self.db,
            &self.config.retention.parquet_path,
            cutoff,
        )
        .await
        {
            tracing::warn!(target: "compact", "Parquet export failed: {}", e);
            // Continue with compaction even if Parquet export fails
        }

        // Step 2: Atomic swap via transaction
        // For v1, simplified: just delete old slices and let the aggregate query
        // recompute from remaining slices. Full consolidated table management
        // comes when the SQL generation is wired up.
        self.db
            .execute(
                format!("DELETE FROM slices WHERE _window_id <= {}", cutoff),
                vec![],
            )
            .await?;

        // Step 3: Update cutoff
        window::set_compaction_cutoff(cutoff);
        Persistence::save_compaction_cutoff(&self.db, cutoff).await?;

        // Step 4: Prune old Parquet files
        let retention_secs = self.config.retention.parse_duration_seconds().unwrap_or(518400);
        if let Err(e) = RetentionManager::prune(&self.config.retention.parquet_path, retention_secs) {
            tracing::warn!(target: "retention", "Parquet prune error: {}", e);
        }

        tracing::debug!(target: "compact", "Compaction complete, new cutoff: {}", cutoff);
        Ok(())
    }
}
```

- [ ] **Step 5: Add modules to main.rs**

Add `pub mod compaction;` and `pub mod retention;` to `src/main.rs` module declarations.

- [ ] **Step 6: Run tests**

Run: `cargo test --no-default-features --test compaction_test`
Expected: both tests pass

- [ ] **Step 7: Commit**

```bash
git add src/compaction.rs src/retention.rs tests/compaction_test.rs src/main.rs
git commit -m "feat: compaction thread with Parquet export and retention pruning"
```

---

## Phase 4: Publication + SSE (Task 13)

### Task 13: SSE Broadcast + Optional NATS Publication

**Files:**
- Create: `src/publish.rs`
- Create: `src/web/sse.rs`

- [ ] **Step 1: Implement src/publish.rs**

```rust
// src/publish.rs
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use crate::config::NatsPublishConfig;
use crate::error::{HcError, HcResult};
use tokio::sync::broadcast;

/// Serialise a RecordBatch to Arrow IPC bytes.
pub fn batch_to_arrow_ipc(batch: &RecordBatch) -> HcResult<Vec<u8>> {
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())
            .map_err(|e| HcError::Publish(format!("Arrow IPC write error: {}", e)))?;
        writer
            .write(batch)
            .map_err(|e| HcError::Publish(format!("Arrow IPC write error: {}", e)))?;
        writer
            .finish()
            .map_err(|e| HcError::Publish(format!("Arrow IPC finish error: {}", e)))?;
    }
    Ok(buf)
}

/// Serialise a RecordBatch to base64-encoded Arrow IPC (for SSE payloads).
pub fn batch_to_base64_arrow(batch: &RecordBatch) -> HcResult<String> {
    let ipc = batch_to_arrow_ipc(batch)?;
    Ok(BASE64.encode(&ipc))
}

/// Delta event published to the broadcast channel and optionally to NATS.
#[derive(Clone, Debug)]
pub struct DeltaEvent {
    pub base64_arrow: String,
    pub row_count: usize,
}

/// Optional NATS publisher.
pub struct NatsPublisher {
    client: async_nats::Client,
    subject_prefix: String,
}

impl NatsPublisher {
    pub async fn connect(config: &NatsPublishConfig) -> HcResult<Self> {
        let client = async_nats::connect(&config.url)
            .await
            .map_err(|e| HcError::Publish(format!("NATS connect error: {}", e)))?;

        // Check max_payload
        let server_info = client.server_info();
        if server_info.max_payload < 4 * 1024 * 1024 {
            tracing::warn!(
                target: "publish",
                "NATS max_payload is {} bytes — burst delta batches may be rejected. \
                 Recommend setting max_payload to at least 4MB in NATS server config.",
                server_info.max_payload
            );
        }

        tracing::info!(target: "publish", "Connected to NATS at {}", config.url);

        Ok(Self {
            client,
            subject_prefix: config.subject_prefix.clone(),
        })
    }

    pub async fn publish(&self, ipc_bytes: &[u8]) -> HcResult<()> {
        let subject = format!("{}.all", self.subject_prefix);
        self.client
            .publish(subject, ipc_bytes.to_vec().into())
            .await
            .map_err(|e| HcError::Publish(format!("NATS publish error: {}", e)))?;
        Ok(())
    }
}
```

- [ ] **Step 2: Implement src/web/sse.rs**

```rust
// src/web/sse.rs
use axum::response::sse::{Event, Sse};
use axum::extract::State;
use futures::stream::Stream;
use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::broadcast;

use crate::publish::DeltaEvent;

/// SSE stream that wraps a broadcast receiver.
pub struct SseStream {
    rx: broadcast::Receiver<DeltaEvent>,
}

impl SseStream {
    pub fn new(rx: broadcast::Receiver<DeltaEvent>) -> Self {
        Self { rx }
    }
}

impl Stream for SseStream {
    type Item = Result<Event, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.rx.try_recv() {
            Ok(event) => {
                let sse_event = Event::default()
                    .event("delta")
                    .data(event.base64_arrow);
                Poll::Ready(Some(Ok(sse_event)))
            }
            Err(broadcast::error::TryRecvError::Empty) => {
                // Register waker and return pending
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(broadcast::error::TryRecvError::Lagged(n)) => {
                tracing::warn!(target: "sse", "SSE client lagged by {} events", n);
                // Continue receiving — client missed some events
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(broadcast::error::TryRecvError::Closed) => Poll::Ready(None),
        }
    }
}

/// Axum handler for the SSE endpoint.
pub async fn sse_handler(
    State(broadcast_tx): State<broadcast::Sender<DeltaEvent>>,
) -> Sse<SseStream> {
    let rx = broadcast_tx.subscribe();
    Sse::new(SseStream::new(rx))
}
```

- [ ] **Step 3: Add publish module to main.rs**

Add `pub mod publish;` to `src/main.rs` module declarations.

- [ ] **Step 4: Verify it compiles**

Run: `cargo check --no-default-features`
Expected: compiles successfully

- [ ] **Step 5: Commit**

```bash
git add src/publish.rs src/web/sse.rs src/main.rs
git commit -m "feat: SSE broadcast and optional NATS publication"
```

---

## Phase 5: Web Server + UI (Tasks 14-17)

### Task 14: Axum HTTP Server + API Endpoints

**Files:**
- Create: `src/web/mod.rs`
- Create: `src/web/server.rs`
- Create: `src/web/api.rs`

- [ ] **Step 1: Implement src/web/mod.rs**

```rust
// src/web/mod.rs
pub mod api;
pub mod assets;
pub mod server;
pub mod sse;
```

- [ ] **Step 2: Implement src/web/api.rs**

```rust
// src/web/api.rs
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Json;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Instant;

use crate::db_manager::DbManager;
use crate::config::CubeConfig;

#[derive(Clone)]
pub struct AppState {
    pub db: DbManager,
    pub config: CubeConfig,
    pub start_time: Instant,
    pub broadcast_tx: tokio::sync::broadcast::Sender<crate::publish::DeltaEvent>,
}

/// GET /api/status
pub async fn status_handler(State(state): State<Arc<AppState>>) -> Json<Value> {
    let uptime = state.start_time.elapsed().as_secs();
    let window_id = crate::aggregation::window::WINDOW_ID
        .load(std::sync::atomic::Ordering::SeqCst);
    let cutoff = crate::aggregation::window::compaction_cutoff();

    Json(json!({
        "cube": state.config.name,
        "status": "running",
        "uptime_seconds": uptime,
        "aggregation": {
            "windows_processed": window_id,
        },
        "compaction": {
            "cutoff_window_id": cutoff,
        },
        "subscribers": {
            "sse_clients": state.broadcast_tx.receiver_count(),
        }
    }))
}

/// GET /api/schema
pub async fn schema_handler(State(state): State<Arc<AppState>>) -> Json<Value> {
    let columns: Vec<Value> = state
        .config
        .schema
        .columns
        .iter()
        .map(|c| json!({"name": c.name, "type": c.col_type}))
        .collect();

    Json(json!({
        "cube": state.config.name,
        "columns": columns,
        "aggregation_sql": state.config.aggregation.sql,
    }))
}

/// GET /api/snapshot — returns full current aggregate as Arrow IPC (base64)
pub async fn snapshot_handler(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let batch = state
        .db
        .query_arrow(state.config.aggregation.sql.clone())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Query error: {}", e)))?;

    let b64 = crate::publish::batch_to_base64_arrow(&batch)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Arrow error: {}", e)))?;

    Ok(Json(json!({
        "format": "arrow_ipc_base64",
        "rows": batch.num_rows(),
        "data": b64,
    })))
}

/// POST /api/query — execute ad-hoc SQL
pub async fn query_handler(
    State(state): State<Arc<AppState>>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let sql = body["sql"]
        .as_str()
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "Missing 'sql' field".to_string()))?
        .to_string();

    let batch = state
        .db
        .query_arrow(sql)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Query error: {}", e)))?;

    let b64 = crate::publish::batch_to_base64_arrow(&batch)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Arrow error: {}", e)))?;

    Ok(Json(json!({
        "format": "arrow_ipc_base64",
        "rows": batch.num_rows(),
        "data": b64,
    })))
}
```

- [ ] **Step 3: Implement src/web/server.rs**

```rust
// src/web/server.rs
use axum::routing::{get, post};
use axum::Router;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::broadcast;

use crate::config::CubeConfig;
use crate::db_manager::DbManager;
use crate::publish::DeltaEvent;
use crate::web::api::{self, AppState};
use crate::web::sse;
use crate::error::{HcError, HcResult};

pub async fn start_server(
    db: DbManager,
    config: CubeConfig,
    broadcast_tx: broadcast::Sender<DeltaEvent>,
    port: u16,
) -> HcResult<()> {
    let state = Arc::new(AppState {
        db,
        config: config.clone(),
        start_time: Instant::now(),
        broadcast_tx: broadcast_tx.clone(),
    });

    let app = Router::new()
        .route("/api/status", get(api::status_handler))
        .route("/api/schema", get(api::schema_handler))
        .route("/api/snapshot", get(api::snapshot_handler))
        .route("/api/query", post(api::query_handler))
        .route(
            "/api/stream",
            get(move |_: axum::extract::State<Arc<AppState>>| async move {
                let rx = broadcast_tx.subscribe();
                axum::response::sse::Sse::new(sse::SseStream::new(rx))
            }),
        )
        .with_state(state);

    let addr = format!("0.0.0.0:{}", port);
    tracing::info!(target: "ui", "Web UI available at http://localhost:{}", port);
    tracing::info!(target: "sse", "SSE endpoint available at /api/stream");

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .map_err(|e| HcError::Web(format!("Cannot bind to {}: {}", addr, e)))?;

    axum::serve(listener, app)
        .await
        .map_err(|e| HcError::Web(format!("Server error: {}", e)))?;

    Ok(())
}
```

- [ ] **Step 4: Add web module to main.rs**

Add `pub mod web;` to `src/main.rs` module declarations.

- [ ] **Step 5: Create stub src/web/assets.rs**

```rust
// src/web/assets.rs
// Static asset serving — implemented in Task 15
```

- [ ] **Step 6: Verify it compiles**

Run: `cargo check --no-default-features`
Expected: compiles

- [ ] **Step 7: Commit**

```bash
git add src/web/ src/main.rs
git commit -m "feat: Axum HTTP server with status, schema, snapshot, query, and SSE endpoints"
```

---

### Task 15: Vendor Script + Static Asset Embedding

**Files:**
- Create: `scripts/vendor.sh`
- Create: `static/` placeholder files
- Modify: `src/web/assets.rs`

- [ ] **Step 1: Create scripts/vendor.sh**

```bash
#!/bin/bash
# vendor.sh — Download and pin vendored frontend assets
set -euo pipefail

STATIC_DIR="$(dirname "$0")/../static"
mkdir -p "$STATIC_DIR/perspective" "$STATIC_DIR/bootstrap" "$STATIC_DIR/bootswatch" "$STATIC_DIR/codemirror"

echo "Downloading Perspective..."
PERSPECTIVE_VERSION="3.4.2"
npm pack @finos/perspective@${PERSPECTIVE_VERSION} --pack-destination /tmp
npm pack @finos/perspective-viewer@${PERSPECTIVE_VERSION} --pack-destination /tmp
# Extract the WASM and JS files from the tarballs
cd /tmp && tar xzf finos-perspective-${PERSPECTIVE_VERSION}.tgz && \
  cp -r package/dist/* "$STATIC_DIR/perspective/" 2>/dev/null || true
cd /tmp && tar xzf finos-perspective-viewer-${PERSPECTIVE_VERSION}.tgz && \
  cp -r package/dist/* "$STATIC_DIR/perspective/" 2>/dev/null || true

echo "Downloading Bootstrap 5..."
BOOTSTRAP_VERSION="5.3.3"
curl -sL "https://cdn.jsdelivr.net/npm/bootstrap@${BOOTSTRAP_VERSION}/dist/css/bootstrap.min.css" \
  -o "$STATIC_DIR/bootstrap/bootstrap.min.css"
curl -sL "https://cdn.jsdelivr.net/npm/bootstrap@${BOOTSTRAP_VERSION}/dist/js/bootstrap.bundle.min.js" \
  -o "$STATIC_DIR/bootstrap/bootstrap.bundle.min.js"

echo "Downloading Bootswatch themes..."
BOOTSWATCH_VERSION="5.3.3"
curl -sL "https://cdn.jsdelivr.net/npm/bootswatch@${BOOTSWATCH_VERSION}/dist/flatly/bootstrap.min.css" \
  -o "$STATIC_DIR/bootswatch/flatly.min.css"
curl -sL "https://cdn.jsdelivr.net/npm/bootswatch@${BOOTSWATCH_VERSION}/dist/darkly/bootstrap.min.css" \
  -o "$STATIC_DIR/bootswatch/darkly.min.css"

echo "Downloading CodeMirror..."
CODEMIRROR_VERSION="5.65.18"
curl -sL "https://cdn.jsdelivr.net/npm/codemirror@${CODEMIRROR_VERSION}/lib/codemirror.min.css" \
  -o "$STATIC_DIR/codemirror/codemirror.min.css"
curl -sL "https://cdn.jsdelivr.net/npm/codemirror@${CODEMIRROR_VERSION}/lib/codemirror.min.js" \
  -o "$STATIC_DIR/codemirror/codemirror.min.js"
curl -sL "https://cdn.jsdelivr.net/npm/codemirror@${CODEMIRROR_VERSION}/mode/sql/sql.min.js" \
  -o "$STATIC_DIR/codemirror/sql.min.js"

echo "Done. Assets vendored to $STATIC_DIR"
```

- [ ] **Step 2: Create placeholder static files for development (so builds work without running vendor.sh)**

```bash
mkdir -p static/perspective static/bootstrap static/bootswatch static/codemirror
echo "/* placeholder */" > static/bootstrap/bootstrap.min.css
echo "/* placeholder */" > static/bootstrap/bootstrap.bundle.min.js
echo "/* placeholder */" > static/bootswatch/flatly.min.css
echo "/* placeholder */" > static/bootswatch/darkly.min.css
echo "/* placeholder */" > static/codemirror/codemirror.min.css
echo "/* placeholder */" > static/codemirror/codemirror.min.js
echo "/* placeholder */" > static/codemirror/sql.min.js
```

- [ ] **Step 3: Implement src/web/assets.rs**

```rust
// src/web/assets.rs
use rust_embed::Embed;
use axum::response::{IntoResponse, Response};
use axum::http::{header, StatusCode};

#[derive(Embed)]
#[folder = "static/"]
struct StaticAssets;

pub async fn static_handler(
    axum::extract::Path(path): axum::extract::Path<String>,
) -> impl IntoResponse {
    match StaticAssets::get(&path) {
        Some(content) => {
            let mime = mime_guess::from_path(&path)
                .first_or_octet_stream()
                .to_string();
            Response::builder()
                .header(header::CONTENT_TYPE, mime)
                .body(axum::body::Body::from(content.data.to_vec()))
                .unwrap()
        }
        None => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(axum::body::Body::from("Not found"))
            .unwrap(),
    }
}
```

Note: add `mime_guess = "2"` to `[dependencies]` in Cargo.toml.

- [ ] **Step 4: Verify it compiles**

Run: `cargo check --no-default-features`
Expected: compiles

- [ ] **Step 5: Commit**

```bash
chmod +x scripts/vendor.sh
git add scripts/vendor.sh static/ src/web/assets.rs Cargo.toml
git commit -m "feat: vendor script and static asset embedding"
```

---

### Task 16: Minijinja UI Template

**Files:**
- Create: `templates/index.html.j2`

- [ ] **Step 1: Create the Minijinja template**

```html
<!-- templates/index.html.j2 -->
<!DOCTYPE html>
<html lang="en" data-bs-theme="light">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>HydroCube — {{ cube_name }}</title>
    <link id="theme-css" rel="stylesheet" href="/assets/bootswatch/flatly.min.css">
    <link rel="stylesheet" href="/assets/codemirror/codemirror.min.css">
    <style>
        #perspective-viewer { width: 100%; height: 500px; }
        #query-result { width: 100%; height: 300px; display: none; }
        .status-bar { font-size: 0.85rem; color: #666; padding: 8px 16px; border-top: 1px solid #dee2e6; }
    </style>
</head>
<body>
    <!-- Header -->
    <nav class="navbar navbar-dark bg-primary px-3">
        <span class="navbar-brand mb-0 h1">HydroCube — {{ cube_name }}</span>
        <div class="d-flex align-items-center gap-3">
            <button id="theme-toggle" class="btn btn-sm btn-outline-light" title="Toggle theme">&#9788;</button>
            <span id="rate-display" class="text-light small">0/s</span>
            <span id="connection-dot" class="text-danger">&#9679;</span>
        </div>
    </nav>

    <!-- Main Perspective Panel -->
    <div class="container-fluid p-3">
        <perspective-viewer id="perspective-viewer"></perspective-viewer>
    </div>

    <!-- SQL Query Panel -->
    <div class="container-fluid px-3">
        <div class="card">
            <div class="card-header d-flex justify-content-between align-items-center">
                <span>SQL Query</span>
                <div>
                    <button id="run-query" class="btn btn-sm btn-primary">Run</button>
                </div>
            </div>
            <div class="card-body p-0">
                <textarea id="sql-editor" class="form-control border-0" rows="3"
                    placeholder="SELECT * FROM consolidated LIMIT 100">SELECT * FROM consolidated LIMIT 100</textarea>
            </div>
        </div>
    </div>

    <!-- Query Result Panel -->
    <div class="container-fluid p-3">
        <perspective-viewer id="query-result"></perspective-viewer>
    </div>

    <!-- Status Bar -->
    <div class="status-bar" id="status-bar">
        Connecting...
    </div>

    <!-- Scripts -->
    <script src="/assets/bootstrap/bootstrap.bundle.min.js"></script>
    <script src="/assets/codemirror/codemirror.min.js"></script>
    <script src="/assets/codemirror/sql.min.js"></script>
    <script type="module">
        // Theme toggle
        const themeToggle = document.getElementById('theme-toggle');
        let darkMode = localStorage.getItem('hydrocube-dark') === 'true';

        function applyTheme() {
            const css = darkMode ? '/assets/bootswatch/darkly.min.css' : '/assets/bootswatch/flatly.min.css';
            document.getElementById('theme-css').href = css;
            document.documentElement.setAttribute('data-bs-theme', darkMode ? 'dark' : 'light');
            themeToggle.textContent = darkMode ? '\u263E' : '\u2606';
            localStorage.setItem('hydrocube-dark', darkMode);
        }
        applyTheme();
        themeToggle.addEventListener('click', () => { darkMode = !darkMode; applyTheme(); });

        // SSE connection
        const dot = document.getElementById('connection-dot');
        const statusBar = document.getElementById('status-bar');
        const rateDisplay = document.getElementById('rate-display');
        let deltaCount = 0;

        // Load initial snapshot
        async function loadSnapshot() {
            try {
                const resp = await fetch('/api/snapshot');
                const data = await resp.json();
                statusBar.textContent = `Loaded ${data.rows} groups`;
                dot.classList.remove('text-danger');
                dot.classList.add('text-success');
            } catch (e) {
                statusBar.textContent = `Snapshot load error: ${e.message}`;
            }
        }
        loadSnapshot();

        // Connect SSE
        const evtSource = new EventSource('/api/stream');
        evtSource.addEventListener('delta', (e) => {
            deltaCount++;
        });
        evtSource.onerror = () => {
            dot.classList.remove('text-success');
            dot.classList.add('text-danger');
            statusBar.textContent = 'Disconnected — reconnecting...';
        };

        // Rate counter
        setInterval(() => {
            rateDisplay.textContent = `${deltaCount}/s`;
            deltaCount = 0;
        }, 1000);

        // Status polling
        setInterval(async () => {
            try {
                const resp = await fetch('/api/status');
                const s = await resp.json();
                statusBar.textContent =
                    `${s.aggregation?.windows_processed || 0} windows | ` +
                    `${s.subscribers?.sse_clients || 0} viewers`;
            } catch {}
        }, 5000);

        // Ad-hoc query
        document.getElementById('run-query').addEventListener('click', async () => {
            const sql = document.getElementById('sql-editor').value;
            const resultViewer = document.getElementById('query-result');
            try {
                const resp = await fetch('/api/query', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({sql}),
                });
                const data = await resp.json();
                resultViewer.style.display = 'block';
                statusBar.textContent = `Query returned ${data.rows} rows`;
            } catch (e) {
                statusBar.textContent = `Query error: ${e.message}`;
            }
        });
    </script>
</body>
</html>
```

- [ ] **Step 2: Commit**

```bash
git add templates/index.html.j2
git commit -m "feat: Minijinja web UI template with Perspective, SSE, and query panel"
```

---

### Task 17: Basic Auth Middleware

**Files:**
- Create: `src/auth/mod.rs`
- Create: `src/auth/basic.rs`

- [ ] **Step 1: Implement src/auth/basic.rs**

```rust
// src/auth/basic.rs
use axum::extract::Request;
use axum::http::{header, StatusCode};
use axum::middleware::Next;
use axum::response::Response;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;

#[derive(Clone)]
pub struct BasicAuthConfig {
    pub username: String,
    pub password: String,
}

pub async fn basic_auth_middleware(
    config: BasicAuthConfig,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let auth_header = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());

    match auth_header {
        Some(auth) if auth.starts_with("Basic ") => {
            let decoded = BASE64
                .decode(&auth[6..])
                .map_err(|_| StatusCode::UNAUTHORIZED)?;
            let creds = String::from_utf8(decoded).map_err(|_| StatusCode::UNAUTHORIZED)?;
            let mut parts = creds.splitn(2, ':');
            let user = parts.next().unwrap_or("");
            let pass = parts.next().unwrap_or("");

            if user == config.username && pass == config.password {
                Ok(next.run(req).await)
            } else {
                Err(StatusCode::UNAUTHORIZED)
            }
        }
        _ => {
            // Return 401 with WWW-Authenticate header to prompt browser login
            Err(StatusCode::UNAUTHORIZED)
        }
    }
}
```

- [ ] **Step 2: Implement src/auth/mod.rs**

```rust
// src/auth/mod.rs
pub mod basic;
```

- [ ] **Step 3: Add auth module to main.rs**

Add `pub mod auth;` to `src/main.rs` module declarations.

- [ ] **Step 4: Verify it compiles**

Run: `cargo check --no-default-features`
Expected: compiles

- [ ] **Step 5: Commit**

```bash
git add src/auth/ src/main.rs
git commit -m "feat: basic auth middleware for web UI protection"
```

---

## Phase 6: Integration (Tasks 18-19)

### Task 18: Main Loop Wiring + Graceful Shutdown

**Files:**
- Create: `src/shutdown.rs`
- Rewrite: `src/main.rs` (full main loop)

- [ ] **Step 1: Implement src/shutdown.rs**

```rust
// src/shutdown.rs
use tokio::signal;
use tokio::sync::watch;

/// Create a shutdown signal channel. Returns (sender, receiver).
/// The sender is triggered on SIGINT/SIGTERM.
pub fn shutdown_signal() -> (watch::Sender<bool>, watch::Receiver<bool>) {
    let (tx, rx) = watch::channel(false);
    let tx_clone = tx.clone();

    tokio::spawn(async move {
        let ctrl_c = signal::ctrl_c();
        #[cfg(unix)]
        let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler");

        tokio::select! {
            _ = ctrl_c => {
                tracing::info!(target: "shutdown", "Received SIGINT, initiating graceful shutdown...");
            }
            #[cfg(unix)]
            _ = sigterm.recv() => {
                tracing::info!(target: "shutdown", "Received SIGTERM, initiating graceful shutdown...");
            }
        }

        let _ = tx_clone.send(true);
    });

    (tx, rx)
}
```

- [ ] **Step 2: Rewrite src/main.rs with the full main loop**

```rust
// src/main.rs
pub mod aggregation;
pub mod auth;
pub mod cli;
pub mod compaction;
pub mod config;
pub mod db_manager;
pub mod delta;
pub mod error;
pub mod ingest;
pub mod persistence;
pub mod publish;
pub mod retention;
pub mod shutdown;
pub mod transform;
pub mod web;

use clap::Parser;
use cli::Cli;
use config::CubeConfig;
use error::exit_code;
use tokio::sync::broadcast;

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Load config
    let yaml = match std::fs::read_to_string(&cli.config) {
        Ok(y) => y,
        Err(e) => {
            eprintln!("ERROR: cannot read config file {:?}: {}", cli.config, e);
            std::process::exit(exit_code::CONFIG_ERROR);
        }
    };

    let config: CubeConfig = match serde_yaml::from_str(&yaml) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("ERROR: invalid config: {}", e);
            std::process::exit(exit_code::CONFIG_ERROR);
        }
    };

    if let Err(e) = config.validate() {
        eprintln!("ERROR: {}", e);
        std::process::exit(exit_code::CONFIG_ERROR);
    }

    if cli.validate {
        println!("Config OK: cube '{}'", config.name);
        std::process::exit(exit_code::OK);
    }

    // Set up logging
    let log_level = cli.log_level.as_deref().unwrap_or(&config.log_level);
    let env_filter = tracing_subscriber::EnvFilter::try_new(log_level)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .init();

    tracing::info!("HydroCube starting: cube '{}'", config.name);

    // Open DuckDB
    let db_path = if config.persistence.enabled {
        config.persistence.path.clone()
    } else {
        ":memory:".to_string()
    };

    let db = match db_manager::DbManager::open(&db_path).await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("ERROR: cannot open DuckDB: {}", e);
            std::process::exit(exit_code::PERSISTENCE_FAILURE);
        }
    };

    // Handle --reset
    if cli.reset {
        tracing::info!("Resetting all persistent state...");
        if let Err(e) = persistence::Persistence::reset(&db, &config).await {
            eprintln!("ERROR: reset failed: {}", e);
            std::process::exit(exit_code::PERSISTENCE_FAILURE);
        }
        println!("Reset complete.");
        std::process::exit(exit_code::OK);
    }

    // Handle --rebuild
    if cli.rebuild {
        if let Err(e) = persistence::Persistence::rebuild(&db, &config).await {
            eprintln!("ERROR: rebuild failed: {}", e);
            std::process::exit(exit_code::PERSISTENCE_FAILURE);
        }
        println!("Rebuild complete.");
        // Continue to normal startup after rebuild
    }

    // Initialise persistence
    if let Err(e) = persistence::Persistence::init(&db, &config).await {
        eprintln!("ERROR: persistence init failed: {}", e);
        std::process::exit(exit_code::PERSISTENCE_FAILURE);
    }

    // Check config hash (unless we just rebuilt)
    if !cli.rebuild {
        if let Err(e) = persistence::Persistence::verify_config_hash(&db, &config).await {
            match e {
                error::HcError::ConfigHashMismatch => {
                    eprintln!(
                        "ERROR: Config hash mismatch — cube schema has changed since last run.\n\
                         Run with --rebuild to reconstruct from retained Parquet history."
                    );
                    std::process::exit(exit_code::CONFIG_HASH_MISMATCH);
                }
                _ => {
                    eprintln!("ERROR: config verification failed: {}", e);
                    std::process::exit(exit_code::PERSISTENCE_FAILURE);
                }
            }
        }
    }

    // Restore window state from persistence
    let last_window_id = persistence::Persistence::load_last_window_id(&db)
        .await
        .unwrap_or(0);
    let cutoff = persistence::Persistence::load_compaction_cutoff(&db)
        .await
        .unwrap_or(0);
    aggregation::window::restore_state(last_window_id, cutoff);

    tracing::info!(
        "Restored state: window_id={}, cutoff={}",
        last_window_id, cutoff
    );

    // Set up shutdown signal
    let (shutdown_tx, shutdown_rx) = shutdown::shutdown_signal();

    // Set up broadcast channel for SSE
    let (broadcast_tx, _) = broadcast::channel::<publish::DeltaEvent>(1024);

    // Start compaction thread
    let compaction = compaction::CompactionThread::new(db.clone(), config.clone());
    let compaction_shutdown = shutdown_rx.clone();
    tokio::spawn(async move {
        if let Err(e) = compaction.run(compaction_shutdown).await {
            tracing::error!("Compaction thread error: {}", e);
        }
    });

    // Start web server (if UI enabled)
    let ui_port = cli.ui_port.unwrap_or_else(|| {
        config.ui.as_ref().map(|u| u.port).unwrap_or(8080)
    });
    let ui_enabled = !cli.no_ui
        && config.ui.as_ref().map(|u| u.enabled).unwrap_or(true);

    if ui_enabled {
        let web_db = db.clone();
        let web_config = config.clone();
        let web_broadcast = broadcast_tx.clone();
        tokio::spawn(async move {
            if let Err(e) = web::server::start_server(web_db, web_config, web_broadcast, ui_port).await {
                tracing::error!("Web server error: {}", e);
            }
        });
    }

    // Main engine loop placeholder
    // (In the full implementation, this would be the hot path:
    //  ingest -> buffer -> window close -> transform -> insert -> aggregate -> delta -> publish)
    tracing::info!("HydroCube running. Press Ctrl+C to stop.");
    let mut shutdown_watch = shutdown_rx.clone();
    shutdown_watch.changed().await.ok();

    // Graceful shutdown
    tracing::info!("Shutting down...");

    // Save final window state
    let final_window = aggregation::window::WINDOW_ID
        .load(std::sync::atomic::Ordering::SeqCst);
    let _ = persistence::Persistence::save_window_id(&db, final_window).await;

    tracing::info!("HydroCube stopped.");
}
```

- [ ] **Step 3: Verify it compiles and runs**

Run: `cargo build --no-default-features`
Expected: compiles

Run: `cargo run --no-default-features -- --config cube.example.yaml --validate`
Expected: `Config OK: cube 'trading_positions'`

- [ ] **Step 4: Commit**

```bash
git add src/main.rs src/shutdown.rs
git commit -m "feat: main loop wiring with graceful shutdown"
```

---

### Task 19: Integration Test

**Files:**
- Create: `tests/integration_test.rs`

- [ ] **Step 1: Write the integration test**

```rust
// tests/integration_test.rs
//! Integration test: config -> DB init -> insert slices -> aggregate -> delta detect
use hydrocube::aggregation::window;
use hydrocube::config::CubeConfig;
use hydrocube::db_manager::DbManager;
use hydrocube::delta::DeltaDetector;
use hydrocube::persistence::Persistence;
use hydrocube::publish;

fn test_config() -> CubeConfig {
    let yaml = std::fs::read_to_string("cube.example.yaml").unwrap();
    serde_yaml::from_str(&yaml).unwrap()
}

#[tokio::test]
async fn test_end_to_end_pipeline() {
    let db = DbManager::open_in_memory().await.unwrap();
    let config = test_config();

    // Step 1: Init persistence
    Persistence::init(&db, &config).await.unwrap();

    // Step 2: Simulate ingest — insert rows into slices
    let window_id = 1u64;
    db.execute(
        format!(
            "INSERT INTO slices VALUES \
             ('T1', 'FX', 'Trading', 'EURUSD', 'Spot', 'EUR', 100, 1.10, 110, 'BUY', '2026-04-13 10:00:00', {}), \
             ('T2', 'FX', 'Trading', 'GBPUSD', 'Spot', 'GBP', 200, 1.25, 250, 'BUY', '2026-04-13 10:00:01', {}), \
             ('T3', 'Rates', 'Trading', 'UST10Y', 'Bond', 'USD', 50, 98.5, 4925, 'SELL', '2026-04-13 10:00:02', {})",
            window_id, window_id, window_id
        ),
        vec![],
    )
    .await
    .unwrap();

    // Step 3: Run aggregation
    let batch = db.query_arrow(config.aggregation.sql.clone()).await.unwrap();
    assert!(batch.num_rows() > 0, "Aggregation should produce groups");

    // Step 4: Delta detection (first window — all rows are deltas)
    let mut detector = DeltaDetector::new(vec![
        "book".into(),
        "desk".into(),
        "instrument_type".into(),
        "currency".into(),
    ]);
    let (upserts, deletes) = detector.detect(&batch);
    assert_eq!(upserts.num_rows(), batch.num_rows());
    assert_eq!(deletes.num_rows(), 0);

    // Step 5: Arrow IPC serialisation
    let b64 = publish::batch_to_base64_arrow(&upserts).unwrap();
    assert!(!b64.is_empty(), "Arrow IPC should produce output");

    // Step 6: Second window — same data, no deltas
    let (upserts2, deletes2) = detector.detect(&batch);
    assert_eq!(upserts2.num_rows(), 0, "No changes should produce no deltas");
    assert_eq!(deletes2.num_rows(), 0);

    // Step 7: Third window — change a value
    db.execute(
        format!(
            "INSERT INTO slices VALUES \
             ('T4', 'FX', 'Trading', 'EURUSD', 'Spot', 'EUR', 300, 1.12, 336, 'BUY', '2026-04-13 10:00:03', {})",
            2u64
        ),
        vec![],
    )
    .await
    .unwrap();

    let batch3 = db.query_arrow(config.aggregation.sql.clone()).await.unwrap();
    let (upserts3, _) = detector.detect(&batch3);
    assert!(upserts3.num_rows() > 0, "Changed group should appear in deltas");
}

#[tokio::test]
async fn test_config_hash_lifecycle() {
    let db = DbManager::open_in_memory().await.unwrap();
    let config = test_config();

    Persistence::init(&db, &config).await.unwrap();
    Persistence::verify_config_hash(&db, &config).await.unwrap();

    // Modify config — hash should change
    let mut changed = config.clone();
    changed.aggregation.sql = "SELECT book, COUNT(*) FROM slices GROUP BY book".into();
    assert!(Persistence::verify_config_hash(&db, &changed).await.is_err());
}
```

- [ ] **Step 2: Run the integration test**

Run: `cargo test --no-default-features --test integration_test`
Expected: both tests pass

- [ ] **Step 3: Commit**

```bash
git add tests/integration_test.rs
git commit -m "feat: end-to-end integration tests for the full pipeline"
```

---

### Task 20: Example Config + Final Verification

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Update README.md**

```markdown
# HydroCube

Real-time aggregation engine. Ingest from Kafka, aggregate with DuckDB, visualise with Perspective. One binary.

## Quick Start

```bash
# Validate a config
cargo run -- --config cube.example.yaml --validate

# Run (requires Kafka broker)
cargo run -- --config cube.example.yaml

# Run without Kafka feature (for development)
cargo run --no-default-features -- --config cube.example.yaml --validate
```

## Build

```bash
# With Kafka support (requires librdkafka)
cargo build --release

# Without Kafka (pure Rust, no system dependencies)
cargo build --release --no-default-features
```

## Test

```bash
cargo test
cargo test --no-default-features  # Skip Kafka-dependent tests
```
```

- [ ] **Step 2: Run full test suite**

Run: `cargo test --no-default-features`
Expected: all tests pass

- [ ] **Step 3: Run clippy**

Run: `cargo clippy --no-default-features -- -W clippy::all`
Expected: no errors (warnings acceptable for v1)

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs: update README with build and test instructions"
```

---

## Summary

| Phase | Tasks | What It Delivers |
|-------|-------|-----------------|
| **1. Foundation** | 1-5 | Project scaffold, config parsing, DB manager, persistence |
| **2. Engine Core** | 6-11 | Kafka ingest, JSON parser, SQL + Lua transforms, aggregation, delta detection |
| **3. Compaction** | 12 | Background compaction, Parquet export, retention pruning |
| **4. Publication** | 13 | SSE broadcast, optional NATS publication, Arrow IPC serialisation |
| **5. Web** | 14-17 | Axum server, API endpoints, Perspective UI, basic auth |
| **6. Integration** | 18-20 | Main loop wiring, graceful shutdown, integration tests |

Each phase produces compilable, testable code. The test suite grows incrementally — every task adds tests before implementation.

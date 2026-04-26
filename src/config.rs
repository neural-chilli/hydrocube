// src/config.rs
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::error::{HcError, HcResult};

// ── Top-level ────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Clone)]
pub struct CubeConfig {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub tables: Vec<TableConfig>,
    #[serde(default)]
    pub sources: Vec<SourceConfig>,
    pub window: WindowConfig,
    pub persistence: PersistenceConfig,
    #[serde(default)]
    pub retention: Option<RetentionConfig>,
    #[serde(default)]
    pub drillthrough: DrillThroughConfig,
    #[serde(default)]
    pub delta: DeltaConfig,
    pub aggregation: AggregationConfig,
    #[serde(default)]
    pub publish: Option<NatsPublishOutConfig>,
    #[serde(default)]
    pub auth: Option<AuthConfig>,
    #[serde(default = "default_log_level")]
    pub log_level: String,
}

fn default_log_level() -> String { "info".to_string() }

// ── Tables ───────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Clone)]
pub struct TableConfig {
    pub name: String,
    pub mode: TableMode,
    #[serde(default)]
    pub event_time_column: Option<String>,
    #[serde(default)]
    pub key_columns: Option<Vec<String>>,
    pub schema: SchemaConfig,
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum TableMode {
    Append,
    Replace,
    Reference,
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

// ── Sources ──────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Clone)]
pub struct SourceConfig {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(rename = "type")]
    pub source_type: SourceType,
    pub table: String,
    #[serde(default = "default_format")]
    pub format: DataFormat,
    #[serde(default)]
    pub load: Option<LoadTiming>,
    // File sources
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub path_resolver: Option<LuaBlock>,
    #[serde(default)]
    pub batch_size: Option<usize>,
    // Kafka
    #[serde(default)]
    pub brokers: Option<Vec<String>>,
    #[serde(default)]
    pub topic: Option<String>,
    #[serde(default)]
    pub group_id: Option<String>,
    #[serde(default)]
    pub max_batch_size: Option<usize>,
    #[serde(default)]
    pub batch_wait_ms: Option<u64>,
    // NATS
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub subject: Option<String>,
    #[serde(default)]
    pub mode: Option<NatsMode>,
    #[serde(default)]
    pub stream: Option<String>,
    #[serde(default)]
    pub consumer: Option<String>,
    // Transform + identity
    #[serde(default)]
    pub transform: Option<Vec<TransformStep>>,
    #[serde(default)]
    pub identity_key: Option<String>,
}

fn default_format() -> DataFormat { DataFormat::Json }

#[derive(Debug, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum SourceType { Kafka, Nats, Http, File }

#[derive(Debug, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum DataFormat { Json, Csv, Parquet, JsonLines }

#[derive(Debug, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum LoadTiming { Startup, Reset }

#[derive(Debug, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum NatsMode { Core, Jetstream }

// ── Lua blocks ───────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Clone)]
pub struct LuaBlock {
    pub function: String,
    #[serde(default)]
    pub inline: Option<String>,
    #[serde(default)]
    pub script: Option<String>,
}

// ── Transform steps ──────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum TransformStep {
    #[serde(rename = "sql")]
    Sql { sql: String },
    #[serde(rename = "lua")]
    Lua {
        function: String,
        #[serde(default)]
        inline: Option<String>,
        #[serde(default)]
        script: Option<String>,
        #[serde(default)]
        init: Option<String>,
    },
}

// ── Aggregation hooks ────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Clone)]
pub struct AggregationConfig {
    pub key_columns: Vec<String>,
    #[serde(default)]
    pub dimensions: Option<Vec<String>>,
    #[serde(default)]
    pub measures: Option<Vec<String>>,
    #[serde(default)]
    pub startup: Option<HookConfig>,
    #[serde(default)]
    pub compaction: Option<CompactionHookConfig>,
    pub publish: PublishHookConfig,
    #[serde(default)]
    pub snapshots: Option<Vec<SnapshotConfig>>,
    #[serde(default)]
    pub reset: Option<ResetConfig>,
    #[serde(default)]
    pub housekeeping: Option<Vec<HousekeepingJob>>,
    #[serde(default)]
    pub reaggregation: Option<ReaggregationConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct HookConfig {
    #[serde(default)]
    pub lua: Option<LuaBlock>,
    #[serde(default)]
    pub sql: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct CompactionHookConfig {
    pub interval: String,   // e.g. "60s", "5m", "1h"
    #[serde(default)]
    pub lua: Option<LuaBlock>,
    #[serde(default)]
    pub sql: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct PublishHookConfig {
    pub sql: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SnapshotConfig {
    pub name: String,
    pub schedule: String,
    #[serde(default)]
    pub lua: Option<LuaBlock>,
    pub sql: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ResetConfig {
    pub schedule: String,
    #[serde(default)]
    pub lua: Option<LuaBlock>,
    #[serde(default)]
    pub sql: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct HousekeepingJob {
    pub name: String,
    pub schedule: String,
    pub sql: String,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct ReaggregationConfig {
    #[serde(default)]
    pub schedule: Option<String>,
}

// ── Window / persistence / retention ─────────────────────────────────────────

#[derive(Debug, Deserialize, Clone)]
pub struct WindowConfig {
    pub interval_ms: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct PersistenceConfig {
    pub enabled: bool,
    pub path: String,
    pub flush_interval: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RetentionConfig {
    #[serde(default)]
    pub raw: Option<RawRetentionConfig>,
    #[serde(default)]
    pub aggregates: Option<AggregateRetentionConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RawRetentionConfig {
    pub duration: String,
    #[serde(default)]
    pub parquet_path: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AggregateRetentionConfig {
    #[serde(default = "default_forever")]
    pub duration: String,
}

fn default_forever() -> String { "forever".to_string() }

// ── Drillthrough / delta ──────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Clone)]
pub struct DrillThroughConfig {
    #[serde(default = "default_max_rows")]
    pub max_rows: usize,
}

fn default_max_rows() -> usize { 50_000 }

impl Default for DrillThroughConfig {
    fn default() -> Self { Self { max_rows: 50_000 } }
}

#[derive(Debug, Deserialize, Clone)]
pub struct DeltaConfig {
    #[serde(default)]
    pub epsilon: f64,
}

impl Default for DeltaConfig {
    fn default() -> Self { Self { epsilon: 0.0 } }
}

// ── NATS publish output / auth ────────────────────────────────────────────────

#[derive(Debug, Deserialize, Clone)]
pub struct NatsPublishOutConfig {
    #[serde(default)]
    pub nats: Option<NatsPublishConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct NatsPublishConfig {
    pub url: String,
    pub subject_prefix: String,
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

// ── Helper methods ────────────────────────────────────────────────────────────

impl CubeConfig {
    /// Deterministic hash of fields that invalidate persisted DuckDB state.
    /// Changes here require --rebuild.
    pub fn schema_hash(&self) -> String {
        let mut hasher = Sha256::new();
        for table in &self.tables {
            hasher.update(table.name.as_bytes());
            for col in &table.schema.columns {
                hasher.update(col.name.as_bytes());
                hasher.update(col.col_type.as_bytes());
            }
        }
        hasher.update(self.aggregation.key_columns.join(",").as_bytes());
        hasher.update(self.aggregation.publish.sql.as_bytes());
        if let Some(s) = self.aggregation.startup.as_ref().and_then(|h| h.sql.as_ref()) {
            hasher.update(s.as_bytes());
        }
        format!("{:x}", hasher.finalize())
    }

    /// Validate constraints that serde cannot enforce.
    pub fn validate(&self) -> HcResult<()> {
        if self.window.interval_ms < 100 || self.window.interval_ms > 60_000 {
            return Err(HcError::Config(format!(
                "window.interval_ms must be 100–60000, got {}",
                self.window.interval_ms
            )));
        }
        if self.aggregation.key_columns.is_empty() {
            return Err(HcError::Config(
                "aggregation.key_columns must not be empty".into(),
            ));
        }
        if self.aggregation.publish.sql.trim().is_empty() {
            return Err(HcError::Config(
                "aggregation.publish.sql must not be empty".into(),
            ));
        }
        for table in &self.tables {
            if let TableMode::Replace = table.mode {
                if table.key_columns.is_none() {
                    return Err(HcError::Config(format!(
                        "replace table '{}' must declare key_columns",
                        table.name
                    )));
                }
            }
            if table.schema.columns.is_empty() {
                return Err(HcError::Config(format!(
                    "table '{}' must have at least one column",
                    table.name
                )));
            }
        }
        let mut seen_names = std::collections::HashSet::new();
        for table in &self.tables {
            if !seen_names.insert(&table.name) {
                return Err(HcError::Config(format!(
                    "duplicate table name: '{}'", table.name
                )));
            }
        }
        for src in &self.sources {
            if let Some(r) = &src.path_resolver {
                if src.name.is_none() {
                    return Err(HcError::Config(
                        "sources with path_resolver must declare a name".into(),
                    ));
                }
                if r.inline.is_some() && r.script.is_some() {
                    return Err(HcError::Config(format!(
                        "source '{}': path_resolver cannot declare both inline and script",
                        src.name.as_deref().unwrap_or("?")
                    )));
                }
            }
            if let Some(batch_wait) = src.batch_wait_ms {
                if batch_wait > self.window.interval_ms / 2 {
                    return Err(HcError::Config(format!(
                        "source batch_wait_ms ({}) must not exceed window.interval_ms/2 ({})",
                        batch_wait,
                        self.window.interval_ms / 2
                    )));
                }
            }
        }
        if let Some(c) = &self.aggregation.compaction {
            c.interval_seconds().map_err(|_| HcError::Config(format!(
                "aggregation.compaction.interval is invalid: '{}'", c.interval
            )))?;
        }
        Ok(())
    }

    /// Return the TableConfig for a given table name, if it exists.
    pub fn table(&self, name: &str) -> Option<&TableConfig> {
        self.tables.iter().find(|t| t.name == name)
    }
}

impl RawRetentionConfig {
    /// Parse duration string ("7d", "24h") to seconds.
    pub fn parse_duration_seconds(&self) -> HcResult<u64> {
        parse_duration_str(&self.duration)
    }
}

pub fn parse_duration_str(s: &str) -> HcResult<u64> {
    if let Some(days) = s.strip_suffix('d') {
        let d: u64 = days.parse().map_err(|_| {
            HcError::Config(format!("invalid duration: {s}"))
        })?;
        Ok(d * 86_400)
    } else if let Some(hours) = s.strip_suffix('h') {
        let h: u64 = hours.parse().map_err(|_| {
            HcError::Config(format!("invalid duration: {s}"))
        })?;
        Ok(h * 3_600)
    } else if let Some(mins) = s.strip_suffix('m') {
        let m: u64 = mins.parse().map_err(|_| {
            HcError::Config(format!("invalid duration: {s}"))
        })?;
        Ok(m * 60)
    } else if let Some(secs) = s.strip_suffix('s') {
        secs.parse().map_err(|_| HcError::Config(format!("invalid duration: {s}")))
    } else {
        Err(HcError::Config(format!(
            "duration must end with d/h/m/s, got: {s}"
        )))
    }
}

impl CompactionHookConfig {
    /// Parse the interval string to seconds.
    pub fn interval_seconds(&self) -> HcResult<u64> {
        parse_duration_str(&self.interval)
    }
}

// ── Backward-compatible loader ────────────────────────────────────────────────

/// Raw deserialisation target that can hold both old and new formats.
#[derive(Debug, Deserialize)]
struct RawCubeConfig {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,

    // New format
    #[serde(default)]
    pub tables: Vec<TableConfig>,
    #[serde(default)]
    pub sources: Vec<SourceConfig>,

    // Old format (optional — detected for migration)
    #[serde(default)]
    pub source: Option<OldSourceConfig>,
    #[serde(default)]
    pub schema: Option<SchemaConfig>,

    pub window: WindowConfig,
    pub persistence: PersistenceConfig,
    #[serde(default)]
    pub retention: Option<serde_yaml::Value>,

    #[serde(default)]
    pub drillthrough: DrillThroughConfig,
    #[serde(default)]
    pub delta: DeltaConfig,

    // New format aggregation (optional — may be old style)
    #[serde(default)]
    pub aggregation: Option<serde_yaml::Value>,

    // Old compaction block
    #[serde(default)]
    pub compaction: Option<OldCompactionConfig>,

    #[serde(default)]
    pub publish: Option<NatsPublishOutConfig>,
    #[serde(default)]
    pub auth: Option<AuthConfig>,
    #[serde(default = "default_log_level")]
    pub log_level: String,
}

#[derive(Debug, Deserialize)]
struct OldSourceConfig {
    #[serde(rename = "type")]
    pub source_type: String,
    #[serde(default)]
    pub brokers: Option<Vec<String>>,
    #[serde(default)]
    pub topic: Option<String>,
    #[serde(default)]
    pub group_id: Option<String>,
    #[serde(default = "default_format_str")]
    pub format: String,
}

fn default_format_str() -> String { "json".to_string() }

#[derive(Debug, Deserialize)]
struct OldCompactionConfig {
    pub interval_windows: u64,
}

/// Parse a YAML string into a `CubeConfig`, handling both old and new formats.
pub fn load_config(yaml: &str) -> HcResult<CubeConfig> {
    let raw: RawCubeConfig = serde_yaml::from_str(yaml)
        .map_err(|e| HcError::Config(format!("YAML parse error: {e}")))?;

    let is_old_format = raw.source.is_some() || raw.schema.is_some();

    if is_old_format {
        migrate_old_format(raw)
    } else {
        assemble_new_format(raw)
    }
}

fn migrate_old_format(raw: RawCubeConfig) -> HcResult<CubeConfig> {
    let old_source = raw.source.ok_or_else(|| {
        HcError::Config("old-format config missing 'source' block".into())
    })?;
    let old_schema = raw.schema.ok_or_else(|| {
        HcError::Config("old-format config missing 'schema' block".into())
    })?;

    // Resolve aggregation SQL from old block
    let agg_sql = if let Some(ref agg_val) = raw.aggregation {
        agg_val
            .get("sql")
            .and_then(|v| v.as_str())
            .map(|s| s.to_owned())
            .ok_or_else(|| HcError::Config("old-format aggregation must have 'sql'".into()))?
    } else {
        return Err(HcError::Config("old-format config missing 'aggregation.sql'".into()));
    };

    // Infer key_columns from GROUP BY in the SQL (best-effort)
    let key_columns = infer_key_columns_from_sql(&agg_sql);

    let table = TableConfig {
        name: "slices".to_owned(),
        mode: TableMode::Append,
        event_time_column: None,
        key_columns: None,
        schema: old_schema,
    };

    let source_type = match old_source.source_type.as_str() {
        "kafka" => SourceType::Kafka,
        "nats"  => SourceType::Nats,
        "http"  => SourceType::Http,
        "file"  => SourceType::File,
        other   => return Err(HcError::Config(format!("unknown source type: {other}"))),
    };

    let format = match old_source.format.as_str() {
        "json"      => DataFormat::Json,
        "csv"       => DataFormat::Csv,
        "parquet"   => DataFormat::Parquet,
        "jsonlines" => DataFormat::JsonLines,
        other => return Err(HcError::Config(format!("unknown format: {other}"))),
    };

    let source = SourceConfig {
        name: None,
        source_type,
        table: "slices".to_owned(),
        format,
        load: None,
        path: None,
        path_resolver: None,
        batch_size: None,
        brokers: old_source.brokers,
        topic: old_source.topic,
        group_id: old_source.group_id,
        max_batch_size: None,
        batch_wait_ms: None,
        url: None,
        subject: None,
        mode: None,
        stream: None,
        consumer: None,
        transform: None,
        identity_key: None,
    };

    // Old retention format: { duration: "7d", parquet_path: "/tmp" }
    let retention = if let Some(ret_val) = raw.retention {
        let duration = ret_val.get("duration")
            .and_then(|v| v.as_str())
            .unwrap_or("7d")
            .to_owned();
        let parquet_path = ret_val.get("parquet_path")
            .and_then(|v| v.as_str())
            .map(|s| s.to_owned());
        Some(RetentionConfig {
            raw: Some(RawRetentionConfig { duration, parquet_path }),
            aggregates: Some(AggregateRetentionConfig { duration: "forever".to_owned() }),
        })
    } else {
        None
    };

    Ok(CubeConfig {
        name: raw.name,
        description: raw.description,
        tables: vec![table],
        sources: vec![source],
        window: raw.window,
        persistence: raw.persistence,
        retention,
        drillthrough: raw.drillthrough,
        delta: raw.delta,
        aggregation: AggregationConfig {
            key_columns,
            dimensions: None,
            measures: None,
            startup: None,
            compaction: None,
            publish: PublishHookConfig { sql: agg_sql },
            snapshots: None,
            reset: None,
            housekeeping: None,
            reaggregation: None,
        },
        publish: raw.publish,
        auth: raw.auth,
        log_level: raw.log_level,
    })
}

fn assemble_new_format(raw: RawCubeConfig) -> HcResult<CubeConfig> {
    let aggregation: AggregationConfig = if let Some(agg_val) = raw.aggregation {
        serde_yaml::from_value(agg_val)
            .map_err(|e| HcError::Config(format!("aggregation parse error: {e}")))?
    } else {
        return Err(HcError::Config("'aggregation' block is required".into()));
    };

    let retention: Option<RetentionConfig> = if let Some(ret_val) = raw.retention {
        Some(serde_yaml::from_value(ret_val)
            .map_err(|e| HcError::Config(format!("retention parse error: {e}")))?)
    } else {
        None
    };

    Ok(CubeConfig {
        name: raw.name,
        description: raw.description,
        tables: raw.tables,
        sources: raw.sources,
        window: raw.window,
        persistence: raw.persistence,
        retention,
        drillthrough: raw.drillthrough,
        delta: raw.delta,
        aggregation,
        publish: raw.publish,
        auth: raw.auth,
        log_level: raw.log_level,
    })
}

/// Best-effort GROUP BY extraction for old-format migration.
fn infer_key_columns_from_sql(sql: &str) -> Vec<String> {
    let upper = sql.to_uppercase();
    if let Some(pos) = upper.find("GROUP BY") {
        let after = &sql[pos + 8..];
        // Stop at ORDER BY / HAVING / LIMIT
        let stop = ["ORDER BY", "HAVING", "LIMIT"]
            .iter()
            .filter_map(|kw| after.to_uppercase().find(kw))
            .min()
            .unwrap_or(after.len());
        return after[..stop]
            .split(',')
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty())
            .collect();
    }
    vec![]
}

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
        // Parse retention duration to validate it
        self.retention.parse_duration_seconds()?;
        Ok(())
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

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

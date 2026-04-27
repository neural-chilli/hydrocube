// src/persistence.rs
//
// Manages DuckDB table creation, metadata tracking, config hash verification,
// and rebuild from Parquet history.  All DB access goes through DbManager.

use serde_json::Value as JsonValue;
use tracing::{info, warn};

use crate::config::{CubeConfig, TableConfig, TableMode};
use crate::db_manager::DbManager;
use crate::error::{HcError, HcResult};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Create all DuckDB tables idempotently and seed metadata if needed.
///
/// Tables created:
/// - `slices`          — schema columns + `_window_id UBIGINT NOT NULL`
/// - `consolidated`    — same schema as slices (future compaction target)
/// - `_cube_metadata`  — config hash, last window id, compaction cutoff, timestamp
pub async fn init(db: &DbManager, config: &CubeConfig) -> HcResult<()> {
    let schema_cols = schema_column_ddl(config);

    // slices table
    let create_slices = format!(
        "CREATE TABLE IF NOT EXISTS slices ({}, _window_id UBIGINT NOT NULL)",
        schema_cols
    );

    // consolidated table — identical shape to slices
    let create_consolidated = format!(
        "CREATE TABLE IF NOT EXISTS consolidated ({}, _window_id UBIGINT NOT NULL)",
        schema_cols
    );

    // metadata table
    let create_metadata = "CREATE TABLE IF NOT EXISTS _cube_metadata (
        config_hash VARCHAR,
        last_window_id UBIGINT DEFAULT 0,
        compaction_cutoff UBIGINT DEFAULT 0,
        updated_at TIMESTAMP
    )"
    .to_string();

    db.execute(&create_slices, vec![]).await?;
    db.execute(&create_consolidated, vec![]).await?;
    db.execute(&create_metadata, vec![]).await?;

    // Insert initial metadata row if the table is empty.
    let rows = db
        .query_json("SELECT COUNT(*) AS cnt FROM _cube_metadata", vec![])
        .await?;
    let count = rows
        .first()
        .and_then(|r| r.get("cnt"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    if count == 0 {
        let hash = config.schema_hash();
        db.execute(
            "INSERT INTO _cube_metadata (config_hash, last_window_id, compaction_cutoff, updated_at) \
             VALUES (?, 0, 0, NOW())",
            vec![JsonValue::String(hash)],
        )
        .await?;
        info!(target: "persistence", "metadata row seeded");
    }

    info!(target: "persistence", "tables initialised");
    Ok(())
}

/// Compare the stored config hash against the current config.
/// Returns `Err(HcError::ConfigHashMismatch)` if they differ.
pub async fn verify_config_hash(db: &DbManager, config: &CubeConfig) -> HcResult<()> {
    let rows = db
        .query_json("SELECT config_hash FROM _cube_metadata", vec![])
        .await?;

    let stored = rows
        .first()
        .and_then(|r| r.get("config_hash"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_owned())
        .ok_or_else(|| HcError::Persistence("_cube_metadata has no config_hash row".into()))?;

    let current = config.schema_hash();
    if stored != current {
        return Err(HcError::ConfigHashMismatch);
    }
    Ok(())
}

/// Read `last_window_id` from `_cube_metadata`.
pub async fn load_last_window_id(db: &DbManager) -> HcResult<u64> {
    let rows = db
        .query_json("SELECT last_window_id FROM _cube_metadata", vec![])
        .await?;
    Ok(rows
        .first()
        .and_then(|r| r.get("last_window_id"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0))
}

/// Read `compaction_cutoff` from `_cube_metadata`.
pub async fn load_compaction_cutoff(db: &DbManager) -> HcResult<u64> {
    let rows = db
        .query_json("SELECT compaction_cutoff FROM _cube_metadata", vec![])
        .await?;
    Ok(rows
        .first()
        .and_then(|r| r.get("compaction_cutoff"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0))
}

/// Persist a new `last_window_id` to `_cube_metadata`.
pub async fn save_window_id(db: &DbManager, window_id: u64) -> HcResult<()> {
    db.execute(
        "UPDATE _cube_metadata SET last_window_id = ?, updated_at = NOW()",
        vec![JsonValue::Number(window_id.into())],
    )
    .await?;
    Ok(())
}

/// Persist a new `compaction_cutoff` to `_cube_metadata`.
pub async fn save_compaction_cutoff(db: &DbManager, cutoff: u64) -> HcResult<()> {
    db.execute(
        "UPDATE _cube_metadata SET compaction_cutoff = ?, updated_at = NOW()",
        vec![JsonValue::Number(cutoff.into())],
    )
    .await?;
    Ok(())
}

/// Create or verify DuckDB tables for each declared TableConfig.
/// Safe to call on an existing database — uses CREATE TABLE IF NOT EXISTS.
pub async fn init_tables(db: &DbManager, tables: &[TableConfig]) -> HcResult<()> {
    for table in tables {
        let ddl = build_create_table_ddl(table);
        db.execute(&ddl, vec![]).await?;
    }
    Ok(())
}

fn build_create_table_ddl(table: &TableConfig) -> String {
    let mut col_defs: Vec<String> = table
        .schema
        .columns
        .iter()
        .map(|c| format!("  {} {}", c.name, c.col_type))
        .collect();

    match table.mode {
        TableMode::Append => {
            col_defs.push("  _window_id UBIGINT NOT NULL".to_owned());
            format!(
                "CREATE TABLE IF NOT EXISTS {} (\n{}\n)",
                table.name,
                col_defs.join(",\n")
            )
        }
        TableMode::Replace => {
            let key_cols = table
                .key_columns
                .as_ref()
                .expect("replace table must have key_columns");
            col_defs.push(format!("  PRIMARY KEY ({})", key_cols.join(", ")));
            format!(
                "CREATE TABLE IF NOT EXISTS {} (\n{}\n)",
                table.name,
                col_defs.join(",\n")
            )
        }
        TableMode::Reference => {
            format!(
                "CREATE TABLE IF NOT EXISTS {} (\n{}\n)",
                table.name,
                col_defs.join(",\n")
            )
        }
    }
}

/// Drop all managed tables then re-initialise from scratch.
pub async fn reset(db: &DbManager, config: &CubeConfig) -> HcResult<()> {
    db.execute("DROP TABLE IF EXISTS slices", vec![]).await?;
    db.execute("DROP TABLE IF EXISTS consolidated", vec![])
        .await?;
    db.execute("DROP TABLE IF EXISTS _cube_metadata", vec![])
        .await?;
    // Drop user-declared tables too
    for table in &config.tables {
        db.execute(&format!("DROP TABLE IF EXISTS {}", table.name), vec![])
            .await?;
    }
    info!(target: "persistence", "tables dropped for reset");
    init(db, config).await?;
    init_tables(db, &config.tables).await
}

/// Drop `consolidated` and `_cube_metadata`, re-init, then import any Parquet
/// files into `slices` (best-effort — warns if no files found).
pub async fn rebuild(db: &DbManager, config: &CubeConfig) -> HcResult<()> {
    db.execute("DROP TABLE IF EXISTS consolidated", vec![])
        .await?;
    db.execute("DROP TABLE IF EXISTS _cube_metadata", vec![])
        .await?;
    info!(target: "persistence", "consolidated + metadata dropped for rebuild");

    // Re-create the tables and seed metadata.
    let schema_cols = schema_column_ddl(config);
    let create_consolidated = format!(
        "CREATE TABLE IF NOT EXISTS consolidated ({}, _window_id UBIGINT NOT NULL)",
        schema_cols
    );
    let create_metadata = "CREATE TABLE IF NOT EXISTS _cube_metadata (
        config_hash VARCHAR,
        last_window_id UBIGINT DEFAULT 0,
        compaction_cutoff UBIGINT DEFAULT 0,
        updated_at TIMESTAMP
    )"
    .to_string();

    db.execute(&create_consolidated, vec![]).await?;
    db.execute(&create_metadata, vec![]).await?;

    let hash = config.schema_hash();
    db.execute(
        "INSERT INTO _cube_metadata (config_hash, last_window_id, compaction_cutoff, updated_at) \
         VALUES (?, 0, 0, NOW())",
        vec![JsonValue::String(hash)],
    )
    .await?;

    // Best-effort Parquet import.
    let parquet_dir = config
        .retention
        .as_ref()
        .and_then(|r| r.raw.as_ref())
        .and_then(|r| r.parquet_path.clone())
        .unwrap_or_else(|| "/tmp/hydrocube_parquet".to_string());
    let parquet_glob = format!("{}/*.parquet", parquet_dir.trim_end_matches('/'));
    let import_sql = format!(
        "INSERT INTO slices SELECT *, 0 AS _window_id FROM read_parquet('{}', union_by_name=true)",
        parquet_glob
    );
    match db.execute(&import_sql, vec![]).await {
        Ok(n) => info!(target: "persistence", "imported {} rows from Parquet", n),
        Err(e) => warn!(
            target: "persistence",
            "Parquet import skipped (no files or schema mismatch): {}",
            e
        ),
    }

    info!(target: "persistence", "rebuild complete");
    Ok(())
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Build a comma-separated DDL column list from the config schema.
/// Example: `"trade_id VARCHAR, quantity DOUBLE"`
fn schema_column_ddl(config: &CubeConfig) -> String {
    // Use the first append-mode table's schema for backwards compatibility.
    // In later tasks this will be per-table DDL.
    let columns = config
        .tables
        .first()
        .map(|t| t.schema.columns.as_slice())
        .unwrap_or(&[]);
    columns
        .iter()
        .map(|c| format!("{} {}", c.name, c.col_type))
        .collect::<Vec<_>>()
        .join(", ")
}

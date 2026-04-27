// src/ingest/reference.rs
//
// Loads Reference-mode tables from file sources at engine startup.

use crate::config::{RefreshStrategy, SourceType, TableMode};
use crate::db_manager::DbManager;
use crate::error::HcResult;
use crate::ingest::file::load_file_into_table;

/// Load every Reference-mode table that has a File source with
/// `refresh: startup` (or no refresh specified, which defaults to startup).
///
/// Called once during `run_startup_sequence`, after the startup hook runs.
pub async fn load_reference_tables_at_startup(
    db: &DbManager,
    config: &crate::config::CubeConfig,
) -> HcResult<()> {
    for src in &config.sources {
        if src.source_type != SourceType::File {
            continue;
        }

        // Load if refresh is "startup" or absent (file sources default to startup load).
        let should_load = match &src.refresh {
            None => true,
            Some(RefreshStrategy::Named(s)) if s == "startup" => true,
            _ => false,
        };
        if !should_load {
            continue;
        }

        // Only load Reference-mode tables.
        let _table_cfg = match config.table(&src.table) {
            Some(t) if t.mode == TableMode::Reference => t,
            _ => continue,
        };

        let path = match &src.path {
            Some(p) => p.clone(),
            None => {
                tracing::warn!(
                    target: "ingest",
                    "file source for table '{}' has no path — skipping",
                    src.table
                );
                continue;
            }
        };

        tracing::info!(
            target: "ingest",
            "Loading reference table '{}' from {}",
            src.table,
            path
        );
        load_file_into_table(db, &src.table, &path, src.format.clone()).await?;
    }
    Ok(())
}

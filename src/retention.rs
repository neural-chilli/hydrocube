// src/retention.rs
//
// RetentionManager handles exporting slices to Parquet files and pruning
// old Parquet directories based on a retention duration.

use chrono::{NaiveDate, Utc};
use std::fs;
use std::path::Path;
use tracing::{debug, info, warn};

use crate::db_manager::DbManager;
use crate::error::HcResult;

pub struct RetentionManager;

impl RetentionManager {
    /// Export slices with _window_id <= cutoff to a time-partitioned Parquet file.
    ///
    /// Creates a date directory under parquet_path and writes:
    ///   `{parquet_path}/{YYYY-MM-DD}/window_{cutoff:06}.parquet`
    pub async fn export_to_parquet(
        db: &DbManager,
        parquet_path: &str,
        cutoff: u64,
    ) -> HcResult<()> {
        let today = Utc::now().format("%Y-%m-%d").to_string();
        let date_dir = format!("{}/{}", parquet_path.trim_end_matches('/'), today);

        fs::create_dir_all(&date_dir)?;

        let parquet_file = format!("{}/window_{:06}.parquet", date_dir, cutoff);
        let sql = format!(
            "COPY (SELECT * FROM slices WHERE _window_id <= {}) TO '{}'",
            cutoff, parquet_file
        );

        db.execute(&sql, vec![]).await?;

        debug!(
            target: "retention",
            "exported slices up to window {} to {}",
            cutoff, parquet_file
        );

        Ok(())
    }

    /// Delete Parquet date directories older than `retention_seconds` from now.
    ///
    /// Expects directories named in YYYY-MM-DD format under parquet_path.
    /// Warns on delete failures rather than propagating them.
    pub fn prune(parquet_path: &str, retention_seconds: u64) -> HcResult<()> {
        let path = Path::new(parquet_path);
        if !path.exists() {
            return Ok(());
        }

        let now = Utc::now().date_naive();
        let retention_days = retention_seconds / 86400;
        // Calculate the cutoff date: directories strictly before this date are pruned.
        let cutoff_date = now - chrono::Duration::seconds(retention_seconds as i64);

        let entries = fs::read_dir(path)?;
        for entry in entries.flatten() {
            let file_name = entry.file_name();
            let name = file_name.to_string_lossy();

            // Only consider YYYY-MM-DD named directories.
            if let Ok(dir_date) = NaiveDate::parse_from_str(&name, "%Y-%m-%d") {
                if dir_date < cutoff_date {
                    let dir_path = entry.path();
                    match fs::remove_dir_all(&dir_path) {
                        Ok(_) => {
                            info!(
                                target: "retention",
                                "pruned old Parquet directory: {} (older than {}d retention)",
                                dir_path.display(),
                                retention_days
                            );
                        }
                        Err(e) => {
                            warn!(
                                target: "retention",
                                "failed to delete Parquet directory {}: {}",
                                dir_path.display(),
                                e
                            );
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

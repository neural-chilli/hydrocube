// src/compaction.rs
//
// CompactionThread periodically exports old slices to Parquet files,
// deletes them from DuckDB, and updates the compaction cutoff.
//
// All DuckDB access goes through DbManager to allow hot-path interleaving.

use tokio::sync::watch;
use tokio::time::{interval, Duration};
use tracing::{debug, info, warn};

use crate::aggregation::window::{compaction_cutoff, set_compaction_cutoff, WINDOW_ID};
use crate::config::CubeConfig;
use crate::db_manager::DbManager;
use crate::error::HcResult;
use crate::persistence;
use crate::retention::RetentionManager;

/// Safety buffer: keep this many recent windows out of compaction so that
/// in-flight aggregation tasks are never touching rows being deleted.
const SAFETY_BUFFER: u64 = 2;

pub struct CompactionThread {
    db: DbManager,
    config: CubeConfig,
    interval_windows: u64,
}

impl CompactionThread {
    /// Create a new CompactionThread from a DbManager and CubeConfig.
    pub fn new(db: DbManager, config: CubeConfig) -> Self {
        let interval_windows = config.compaction.interval_windows;
        Self {
            db,
            config,
            interval_windows,
        }
    }

    /// Run the compaction loop, ticking every `interval_windows` worth of
    /// wall-clock time (approximated as window interval * interval_windows).
    ///
    /// Exits cleanly when the shutdown watch fires.
    pub async fn run(self, mut shutdown: watch::Receiver<bool>) -> HcResult<()> {
        let tick_ms = self.config.window.interval_ms * self.interval_windows;
        let mut timer = interval(Duration::from_millis(tick_ms));
        // Consume the immediate first tick so we don't compact on startup.
        timer.tick().await;

        info!(
            target: "compaction",
            "compaction thread started (interval_windows={}, tick_ms={})",
            self.interval_windows, tick_ms
        );

        loop {
            tokio::select! {
                _ = timer.tick() => {
                    if let Err(e) = self.compact_cycle().await {
                        warn!(target: "compaction", "compact_cycle error: {}", e);
                    }
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!(target: "compaction", "compaction thread shutting down");
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    /// One compaction cycle:
    ///
    /// 1. Read current window ID from atomic.
    /// 2. Calculate cutoff = current - safety_buffer.
    /// 3. Skip if cutoff <= current compaction_cutoff.
    /// 4. Export to Parquet (best-effort, warn on failure).
    /// 5. Delete old slices from DuckDB.
    /// 6. Update atomic cutoff.
    /// 7. Persist cutoff to metadata.
    /// 8. Prune old Parquet files (best-effort).
    async fn compact_cycle(&self) -> HcResult<()> {
        use std::sync::atomic::Ordering;

        let current_window = WINDOW_ID.load(Ordering::Acquire);

        // Guard against underflow on a fresh start.
        if current_window < SAFETY_BUFFER {
            debug!(
                target: "compaction",
                "current_window={} < safety_buffer={}, skipping",
                current_window, SAFETY_BUFFER
            );
            return Ok(());
        }

        let cutoff = current_window - SAFETY_BUFFER;
        let current_cutoff = compaction_cutoff();

        if cutoff <= current_cutoff {
            debug!(
                target: "compaction",
                "cutoff={} <= current_cutoff={}, nothing to compact",
                cutoff, current_cutoff
            );
            return Ok(());
        }

        info!(
            target: "compaction",
            "compacting up to window {} (current={}, prev_cutoff={})",
            cutoff, current_window, current_cutoff
        );

        // Step 4: Export to Parquet (best-effort).
        let parquet_path = &self.config.retention.parquet_path;
        if let Err(e) = RetentionManager::export_to_parquet(&self.db, parquet_path, cutoff).await {
            warn!(
                target: "compaction",
                "parquet export failed (continuing with delete): {}",
                e
            );
        }

        // Step 5: Delete old slices from DuckDB.
        let delete_sql = format!("DELETE FROM slices WHERE _window_id <= {}", cutoff);
        self.db.execute(&delete_sql, vec![]).await?;

        // Step 6: Advance the in-memory atomic.
        set_compaction_cutoff(cutoff);

        // Step 7: Persist cutoff to metadata.
        persistence::save_compaction_cutoff(&self.db, cutoff).await?;

        info!(
            target: "compaction",
            "compaction complete, cutoff now={}",
            cutoff
        );

        // Step 8: Prune old Parquet directories (best-effort).
        match self.config.retention.parse_duration_seconds() {
            Ok(retention_seconds) => {
                if let Err(e) = RetentionManager::prune(parquet_path, retention_seconds) {
                    warn!(target: "compaction", "retention prune failed: {}", e);
                }
            }
            Err(e) => {
                warn!(
                    target: "compaction",
                    "could not parse retention duration, skipping prune: {}",
                    e
                );
            }
        }

        Ok(())
    }
}

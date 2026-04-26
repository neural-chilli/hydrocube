// src/compaction.rs
//
// CompactionThread periodically runs the user-defined compaction SQL (if
// configured) and/or exports old slices to Parquet, then advances the
// compaction cutoff.  All DuckDB access goes through DbManager.

use tokio::sync::watch;
use tokio::time::{interval, Duration};
use tracing::{debug, info, warn};

use crate::aggregation::window::{compaction_cutoff, set_compaction_cutoff, WINDOW_ID};
use crate::config::CubeConfig;
use crate::db_manager::DbManager;
use crate::error::HcResult;
use crate::hooks::runner::HookRunner;
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
        // In the new config, compaction interval comes from aggregation.compaction.interval.
        // Default to 60 windows if not configured.
        let interval_windows = config.aggregation.compaction.as_ref()
            .and_then(|c| crate::config::parse_duration_str(&c.interval).ok())
            .map(|secs| (secs * 1000 / config.window.interval_ms.max(1)).max(1))
            .unwrap_or(60);
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
    /// 2. Calculate cutoff = current - max(retention_windows, safety_buffer).
    /// 3. Skip if cutoff <= current compaction_cutoff.
    /// 4. Export to Parquet (best-effort, warn on failure).
    /// 5. Delete old slices from DuckDB.
    /// 6. Update atomic cutoff.
    /// 7. Persist cutoff to metadata.
    /// 8. Prune old Parquet files (best-effort).
    async fn compact_cycle(&self) -> HcResult<()> {
        use std::sync::atomic::Ordering;

        let current_window = WINDOW_ID.load(Ordering::Acquire);

        // Calculate how many windows the configured retention period spans.
        // In the new config, raw retention is under retention.raw.duration.
        let raw_retention_secs = self.config.retention.as_ref()
            .and_then(|r| r.raw.as_ref())
            .and_then(|r| r.parse_duration_seconds().ok());
        let retention_windows: u64 = match raw_retention_secs {
            Some(secs) => {
                let interval_secs = (self.config.window.interval_ms as f64 / 1000.0).max(0.001);
                (secs as f64 / interval_secs).ceil() as u64
            }
            None => {
                warn!(target: "compaction", "could not parse retention duration; defaulting to 86400 windows");
                86_400
            }
        };

        // The delete cutoff must respect BOTH the retention period and the
        // safety buffer.  We keep at least `lookback` windows of raw slices.
        let lookback = retention_windows.max(SAFETY_BUFFER);

        if current_window <= lookback {
            debug!(
                target: "compaction",
                "current_window={} <= lookback={} (retention_windows={}, safety_buffer={}), skipping",
                current_window, lookback, retention_windows, SAFETY_BUFFER
            );
            return Ok(());
        }

        let cutoff = current_window - lookback;
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
        let parquet_path_opt = self.config.retention.as_ref()
            .and_then(|r| r.raw.as_ref())
            .and_then(|r| r.parquet_path.clone());
        if let Some(ref parquet_path) = parquet_path_opt {
            if let Err(e) = RetentionManager::export_to_parquet(&self.db, parquet_path, cutoff).await {
                warn!(
                    target: "compaction",
                    "parquet export failed (continuing with delete): {}",
                    e
                );
            }
        }

        // Step 5: Run user-defined compaction SQL (new model) or fall back to
        // deleting from the legacy `slices` table.
        let has_user_sql = self.config.aggregation.compaction.as_ref()
            .and_then(|c| c.sql.as_ref())
            .is_some();

        if has_user_sql {
            // New model: run the user's compaction SQL with {pending.table},
            // {cutoff}, and {new_cutoff} tokens expanded.
            let runner = HookRunner::new(self.config.clone(), self.db.clone());
            if let Err(e) = runner.run_compaction(cutoff).await {
                warn!(target: "compaction", "compaction SQL error: {}", e);
            }
        } else {
            // Legacy model: delete rows from the `slices` table.
            let delete_sql = format!("DELETE FROM slices WHERE _window_id <= {}", cutoff);
            self.db.execute(&delete_sql, vec![]).await?;
        }

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
        if let Some(ref parquet_path) = parquet_path_opt {
            match raw_retention_secs {
                Some(retention_seconds) => {
                    if let Err(e) = RetentionManager::prune(parquet_path, retention_seconds) {
                        warn!(target: "compaction", "retention prune failed: {}", e);
                    }
                }
                None => {
                    warn!(
                        target: "compaction",
                        "could not parse retention duration, skipping prune"
                    );
                }
            }
        }

        Ok(())
    }
}

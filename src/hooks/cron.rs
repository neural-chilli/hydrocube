// src/hooks/cron.rs
//
// Cron expression parsing and next-fire-time calculation.
// Uses the `cron` 0.12 crate which requires 6-field expressions (sec included).
// Standard 5-field expressions are auto-expanded by prepending "0 ".

use chrono::{DateTime, Utc};
use cron::Schedule;
use std::str::FromStr;

use crate::config::CubeConfig;
use crate::db_manager::DbManager;
use crate::error::{HcError, HcResult};

/// Parse a 5-field or 6-field cron expression into a `cron::Schedule`.
///
/// - 5-field input: `"MIN HOUR DOM MON DOW"` → prepend `"0 "` → `"0 MIN HOUR DOM MON DOW"`
/// - 6-field input: `"SEC MIN HOUR DOM MON DOW"` → used as-is
pub fn parse_schedule(expr: &str) -> HcResult<Schedule> {
    let parts: usize = expr.split_whitespace().count();
    let six_field = if parts == 5 {
        format!("0 {}", expr)
    } else {
        expr.to_owned()
    };
    Schedule::from_str(&six_field)
        .map_err(|e| HcError::Config(format!("invalid cron expression '{expr}': {e}")))
}

/// Return the next fire time after `after`, or `None` if the schedule never fires.
pub fn next_fire_after(schedule: &Schedule, after: &DateTime<Utc>) -> Option<DateTime<Utc>> {
    schedule.after(after).next()
}

/// Spawn cron tasks for every snapshot hook that declares a schedule.
/// Returns the join handles so callers can await or drop them.
pub fn spawn_snapshot_cron_tasks(
    config: &CubeConfig,
    db: DbManager,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> Vec<tokio::task::JoinHandle<()>> {
    let snapshots = config.aggregation.snapshots.as_deref().unwrap_or(&[]);
    snapshots
        .iter()
        .filter_map(|snap| {
            let schedule = match parse_schedule(&snap.schedule) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("invalid snapshot schedule '{}': {}", snap.name, e);
                    return None;
                }
            };
            let name = snap.name.clone();
            let cfg = config.clone();
            let db_clone = db.clone();
            let shutdown_clone = shutdown.clone();
            Some(spawn_cron_task(
                schedule,
                move |_| {
                    let runner =
                        crate::hooks::runner::HookRunner::new(cfg.clone(), db_clone.clone());
                    let n = name.clone();
                    async move {
                        if let Err(e) = runner.run_snapshot(&n).await {
                            tracing::error!(target: "cron", "snapshot '{}' error: {}", n, e);
                        }
                    }
                },
                shutdown_clone,
            ))
        })
        .collect()
}

/// Spawn cron tasks for every housekeeping job that declares a schedule.
pub fn spawn_housekeeping_cron_tasks(
    config: &CubeConfig,
    db: DbManager,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> Vec<tokio::task::JoinHandle<()>> {
    let jobs = config.aggregation.housekeeping.as_deref().unwrap_or(&[]);
    jobs.iter()
        .filter_map(|job| {
            let schedule = match parse_schedule(&job.schedule) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("invalid housekeeping schedule '{}': {}", job.name, e);
                    return None;
                }
            };
            let job_name = job.name.clone();
            let cfg = config.clone();
            let db_clone = db.clone();
            let shutdown_clone = shutdown.clone();
            Some(spawn_cron_task(
                schedule,
                move |_| {
                    let runner =
                        crate::hooks::runner::HookRunner::new(cfg.clone(), db_clone.clone());
                    let n = job_name.clone();
                    async move {
                        if let Err(e) = runner.run_housekeeping(&n).await {
                            tracing::error!(target: "cron", "housekeeping '{}' error: {}", n, e);
                        }
                    }
                },
                shutdown_clone,
            ))
        })
        .collect()
}

/// Spawn a tokio task that fires `callback` on each cron tick until the
/// shutdown signal fires. The callback receives the fire time as context.
///
/// Returns a `tokio::task::JoinHandle` that can be awaited or dropped.
pub fn spawn_cron_task<F, Fut>(
    schedule: Schedule,
    mut callback: F,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()>
where
    F: FnMut(DateTime<Utc>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send,
{
    tokio::spawn(async move {
        loop {
            let now = Utc::now();
            let next = match next_fire_after(&schedule, &now) {
                Some(t) => t,
                None => {
                    tracing::warn!(target: "cron", "schedule has no more fire times — stopping task");
                    break;
                }
            };

            let delay = (next - now).to_std().unwrap_or(std::time::Duration::ZERO);

            tokio::select! {
                _ = tokio::time::sleep(delay) => {
                    callback(next).await;
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        tracing::debug!(target: "cron", "cron task shutting down");
                        break;
                    }
                }
            }
        }
    })
}

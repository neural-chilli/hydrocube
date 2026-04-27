// src/startup.rs
//
// Engine initialisation sequences called from main.rs and testable in isolation.

use crate::{
    config::CubeConfig,
    db_manager::DbManager,
    error::HcResult,
    hooks::runner::HookRunner,
    identity::{populate_identity_cache_from_db, MultiTableIdentityCache},
};

/// Run the startup hook (if declared) and return the pre-populated identity
/// cache built from every source that declares an `identity_key`.
///
/// Called once during engine startup, after persistence init and before the
/// ingest channel opens.
pub async fn run_startup_sequence(
    db: &DbManager,
    config: &CubeConfig,
) -> HcResult<MultiTableIdentityCache> {
    let runner = HookRunner::new(config.clone(), db.clone());
    runner.run_startup().await?;

    let mut cache = MultiTableIdentityCache::new();
    for source in &config.sources {
        if let Some(identity_key) = &source.identity_key {
            let keys =
                populate_identity_cache_from_db(db, &source.table, identity_key).await?;
            cache.populate(&source.table, keys);
        }
    }
    Ok(cache)
}

/// Run the reset hook (if declared) and return the re-populated identity cache.
///
/// Called from the CLI `--reset` branch, after `persistence::reset()`.
pub async fn run_reset_sequence(
    db: &DbManager,
    config: &CubeConfig,
) -> HcResult<MultiTableIdentityCache> {
    let runner = HookRunner::new(config.clone(), db.clone());
    runner.run_reset().await?;

    let mut cache = MultiTableIdentityCache::new();
    for source in &config.sources {
        if let Some(identity_key) = &source.identity_key {
            let keys =
                populate_identity_cache_from_db(db, &source.table, identity_key).await?;
            cache.populate(&source.table, keys);
        }
    }
    Ok(cache)
}

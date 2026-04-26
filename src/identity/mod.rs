// src/identity/mod.rs
//
// Per-table identity cache for amendment routing.
// Tracks which identity keys have been seen so Lua transforms can distinguish
// amendments (updates to known rows) from new inserts.

use std::collections::{HashMap, HashSet};

use crate::db_manager::DbManager;
use crate::error::HcResult;

/// Identity cache for a single table.
#[derive(Debug, Default)]
pub struct IdentityCache {
    seen: HashSet<String>,
}

impl IdentityCache {
    pub fn new() -> Self { Self::default() }

    /// Returns true if the key has been seen before.
    pub fn seen(&self, key: &str) -> bool {
        self.seen.contains(key)
    }

    /// Mark a key as seen.
    pub fn mark_seen(&mut self, key: impl Into<String>) {
        self.seen.insert(key.into());
    }

    /// Bulk-populate from an existing list (e.g., loaded from DuckDB at startup).
    pub fn populate(&mut self, keys: Vec<String>) {
        self.seen.extend(keys);
    }

    /// Iterate all seen keys.
    pub fn all_keys(&self) -> impl Iterator<Item = &String> {
        self.seen.iter()
    }

    pub fn len(&self) -> usize { self.seen.len() }
    pub fn is_empty(&self) -> bool { self.seen.is_empty() }
}

/// Identity caches for all tables, keyed by table name.
#[derive(Debug, Default)]
pub struct MultiTableIdentityCache {
    tables: HashMap<String, IdentityCache>,
}

impl MultiTableIdentityCache {
    pub fn new() -> Self { Self::default() }

    pub fn seen(&self, table: &str, key: &str) -> bool {
        self.tables.get(table).map(|c| c.seen(key)).unwrap_or(false)
    }

    pub fn mark_seen(&mut self, table: &str, key: impl Into<String>) {
        self.tables
            .entry(table.to_owned())
            .or_default()
            .mark_seen(key);
    }

    pub fn populate(&mut self, table: &str, keys: Vec<String>) {
        self.tables
            .entry(table.to_owned())
            .or_default()
            .populate(keys);
    }

    pub fn get(&self, table: &str) -> Option<&IdentityCache> {
        self.tables.get(table)
    }

    pub fn get_mut(&mut self, table: &str) -> Option<&mut IdentityCache> {
        self.tables.get_mut(table)
    }
}

/// Query `identity_key_column` from `table_name` and return all distinct values.
/// Used to pre-populate the cache at startup.
pub async fn populate_identity_cache_from_db(
    db: &DbManager,
    table_name: &str,
    identity_key_column: &str,
) -> HcResult<Vec<String>> {
    let sql = format!(
        "SELECT DISTINCT {} AS key FROM {}",
        identity_key_column, table_name
    );
    let rows = db.query_json(&sql, vec![]).await?;
    Ok(rows
        .into_iter()
        .filter_map(|r| r.get("key").and_then(|v| v.as_str()).map(|s| s.to_owned()))
        .collect())
}

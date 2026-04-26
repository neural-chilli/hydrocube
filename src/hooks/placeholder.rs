// src/hooks/placeholder.rs
use std::collections::HashMap;
use chrono::{DateTime, Utc};

use crate::config::TableMode;

/// All runtime values available for placeholder expansion.
#[derive(Debug, Clone)]
pub struct PlaceholderContext {
    pub compaction_cutoff: u64,
    pub new_cutoff: Option<u64>,
    pub now: DateTime<Utc>,
    pub period_start: Option<DateTime<Utc>>,
    /// source_name → resolved paths (used for {path.source_name})
    pub resolved_paths: HashMap<String, Vec<String>>,
    /// table_name → mode (used for {table_name} expansion)
    pub table_modes: HashMap<String, TableMode>,
    /// Pre-resolved publish SQL (used for {publish_sql} in snapshot hooks)
    pub publish_sql: Option<String>,
}

impl PlaceholderContext {
    pub fn new(
        compaction_cutoff: u64,
        new_cutoff: Option<u64>,
        now: DateTime<Utc>,
        period_start: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            compaction_cutoff,
            new_cutoff,
            now,
            period_start,
            resolved_paths: HashMap::new(),
            table_modes: HashMap::new(),
            publish_sql: None,
        }
    }

    /// Expand all known placeholder tokens in `sql`.
    pub fn expand(&self, sql: &str) -> String {
        let mut out = sql.to_owned();

        // ── Date components ───────────────────────────────────────────────────
        let yyyy = self.now.format("%Y").to_string();
        let mm   = self.now.format("%m").to_string();
        let dd   = self.now.format("%d").to_string();
        out = out.replace("{YYYY}", &yyyy);
        out = out.replace("{MM}",   &mm);
        out = out.replace("{DD}",   &dd);
        out = out.replace("{YYYYMMDD}", &format!("{yyyy}{mm}{dd}"));
        out = out.replace("{date}",     &format!("{yyyy}-{mm}-{dd}"));

        // ── Timestamps ────────────────────────────────────────────────────────
        out = out.replace("{now}", &self.now.to_rfc3339());
        if let Some(ps) = &self.period_start {
            out = out.replace("{period_start}", &ps.to_rfc3339());
        }

        // ── Window IDs ────────────────────────────────────────────────────────
        out = out.replace("{cutoff}", &self.compaction_cutoff.to_string());
        if let Some(nc) = self.new_cutoff {
            out = out.replace("{new_cutoff}", &nc.to_string());
        }

        // ── Path tokens: {path.source_name} ──────────────────────────────────
        for (name, paths) in &self.resolved_paths {
            let token = format!("{{path.{name}}}");
            let expanded = if paths.len() == 1 {
                format!("'{}'", paths[0])
            } else {
                let quoted = paths.iter()
                    .map(|p| format!("'{p}'"))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("[{quoted}]")
            };
            out = out.replace(&token, &expanded);
        }

        // ── {publish_sql} ─────────────────────────────────────────────────────
        if let Some(ref psql) = self.publish_sql {
            out = out.replace("{publish_sql}", psql);
        }

        // ── Table tokens — must run last so scalar tokens are already resolved ─
        // Process {pending.table_name} first (more specific), then {table_name}
        let cutoff_str = self.compaction_cutoff.to_string();
        let new_cutoff_str = self.new_cutoff.map(|n| n.to_string()).unwrap_or_default();

        let table_names: Vec<(String, TableMode)> = self.table_modes
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        for (table_name, mode) in &table_names {
            // {pending.table_name}
            let pending_token = format!("{{pending.{table_name}}}");
            if out.contains(&pending_token) {
                let expanded = match mode {
                    TableMode::Append => format!(
                        "(SELECT * FROM {table_name} WHERE _window_id > {cutoff_str} AND _window_id <= {new_cutoff_str})"
                    ),
                    _ => table_name.clone(),
                };
                out = out.replace(&pending_token, &expanded);
            }

            // {table_name}
            let token = format!("{{{table_name}}}");
            if out.contains(&token) {
                let expanded = match mode {
                    TableMode::Append => format!(
                        "(SELECT * FROM {table_name} WHERE _window_id > {cutoff_str})"
                    ),
                    _ => table_name.clone(),
                };
                out = out.replace(&token, &expanded);
            }
        }

        out
    }
}

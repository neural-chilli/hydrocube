// src/hooks/runner.rs
use std::collections::HashMap;

use chrono::Utc;

use crate::aggregation::window::compaction_cutoff;
use crate::config::{CubeConfig, SourceType};
use crate::db_manager::DbManager;
use crate::error::{HcError, HcResult};
use crate::hooks::placeholder::PlaceholderContext;
use crate::ingest::file::resolve_paths_with_lua;

pub struct HookRunner {
    pub config: CubeConfig,
    pub db: DbManager,
    /// Resolved file paths per named source; populated during startup/reset.
    pub resolved_paths: HashMap<String, Vec<String>>,
    /// Current period start (set when reset hook fires).
    pub period_start: Option<chrono::DateTime<Utc>>,
}

impl HookRunner {
    pub fn new(config: CubeConfig, db: DbManager) -> Self {
        Self {
            config,
            db,
            resolved_paths: HashMap::new(),
            period_start: None,
        }
    }

    pub fn db(&self) -> &DbManager {
        &self.db
    }

    fn make_ctx(&self, new_cutoff: Option<u64>) -> PlaceholderContext {
        let mut ctx = PlaceholderContext::new(
            compaction_cutoff(),
            new_cutoff,
            Utc::now(),
            self.period_start,
        );
        ctx.resolved_paths = self.resolved_paths.clone();
        ctx.table_modes = self
            .config
            .tables
            .iter()
            .map(|t| (t.name.clone(), t.mode.clone()))
            .collect();
        ctx
    }

    /// Run the startup hook SQL (if declared). Errors are fatal.
    pub async fn run_startup(&self) -> HcResult<()> {
        let Some(startup) = &self.config.aggregation.startup else {
            return Ok(());
        };
        let Some(sql) = &startup.sql else {
            return Ok(());
        };
        let ctx = self.make_ctx(None);
        let expanded = ctx.expand(sql);
        self.exec_statements(&expanded).await
    }

    /// Return the publish SQL with all tokens expanded, ready for query_arrow.
    pub fn publish_sql_expanded(&self) -> String {
        let ctx = self.make_ctx(None);
        ctx.expand(&self.config.aggregation.publish.sql)
    }

    /// Run compaction hook SQL with a specific new_cutoff value.
    pub async fn run_compaction(&self, new_cutoff: u64) -> HcResult<()> {
        let Some(hook) = &self.config.aggregation.compaction else {
            return Ok(());
        };
        let Some(sql) = &hook.sql else {
            return Ok(());
        };
        let ctx = self.make_ctx(Some(new_cutoff));
        let expanded = ctx.expand(sql);
        self.exec_statements(&expanded).await
    }

    /// Run a named snapshot hook SQL with {publish_sql} available.
    pub async fn run_snapshot(&self, name: &str) -> HcResult<()> {
        let snapshots = self.config.aggregation.snapshots.as_deref().unwrap_or(&[]);
        let Some(snap) = snapshots.iter().find(|s| s.name == name) else {
            return Ok(());
        };
        let mut ctx = self.make_ctx(None);
        ctx.publish_sql = Some(self.publish_sql_expanded());
        let expanded = ctx.expand(&snap.sql);
        self.exec_statements(&expanded).await
    }

    /// Run reset hook SQL. Called after reset sources are loaded.
    pub async fn run_reset(&self) -> HcResult<()> {
        let Some(reset) = &self.config.aggregation.reset else {
            return Ok(());
        };
        let Some(sql) = &reset.sql else {
            return Ok(());
        };
        let ctx = self.make_ctx(None);
        let expanded = ctx.expand(sql);
        self.exec_statements(&expanded).await
    }

    /// Run a housekeeping job SQL. Errors are logged but non-fatal at the call site.
    pub async fn run_housekeeping(&self, job_name: &str) -> HcResult<()> {
        let jobs = self
            .config
            .aggregation
            .housekeeping
            .as_deref()
            .unwrap_or(&[]);
        let Some(job) = jobs.iter().find(|j| j.name == job_name) else {
            return Ok(());
        };
        let ctx = self.make_ctx(None);
        let expanded = ctx.expand(&job.sql);
        self.exec_statements(&expanded).await
    }

    /// Populate `self.resolved_paths` for all named file sources.
    /// Static `path:` sources are recorded directly.
    /// Sources with `path_resolver:` run the Lua function to get paths.
    pub fn resolve_file_paths(&mut self) -> HcResult<()> {
        for src in &self.config.sources {
            if src.source_type != SourceType::File {
                continue;
            }
            let name = match &src.name {
                Some(n) => n.clone(),
                None => continue, // unnamed file sources don't get path tokens
            };

            let paths = if let Some(resolver) = &src.path_resolver {
                // Run Lua to get paths
                let lua_code = resolver
                    .inline
                    .as_deref()
                    .or(resolver.script.as_deref())
                    .ok_or_else(|| {
                        HcError::Config(format!(
                            "source '{name}': path_resolver needs inline or script"
                        ))
                    })?;
                resolve_paths_with_lua(lua_code, &resolver.function)?
            } else if let Some(path) = &src.path {
                vec![path.clone()]
            } else {
                continue; // no path info
            };

            self.resolved_paths.insert(name, paths);
        }
        Ok(())
    }

    /// Execute one or more semicolon-separated SQL statements.
    pub async fn exec_statements(&self, sql: &str) -> HcResult<()> {
        for stmt in sql.split(';') {
            let trimmed = stmt.trim();
            if !trimmed.is_empty() {
                self.db.execute(trimmed, vec![]).await?;
            }
        }
        Ok(())
    }
}

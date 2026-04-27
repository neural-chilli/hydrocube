// src/transform/mod.rs
//
// TransformPipeline: runs a sequence of TransformSteps, feeding each step's
// output into the next.  All DuckDB access is channelled through DbManager.

pub mod lua;
pub mod lua_ctx;
pub mod sql;

use serde_json::Value;

use crate::config::{ColumnDef, TransformStep};
use crate::db_manager::DbManager;
use crate::error::HcResult;
use lua::LuaTransform;
use sql::SqlTransform;

/// Ordered list of transform steps applied to raw messages before they are
/// inserted into the DuckDB slices table.
pub struct TransformPipeline {
    steps: Vec<TransformStep>,
}

impl TransformPipeline {
    /// Build a pipeline from a list of `TransformStep` config values.
    pub fn new(steps: Vec<TransformStep>) -> Self {
        TransformPipeline { steps }
    }

    /// Run all steps in sequence.  Each step's output is the next step's
    /// input.  Returns the final transformed rows.
    pub async fn execute(
        &self,
        db: &DbManager,
        columns: &[ColumnDef],
        input: Vec<Vec<Value>>,
    ) -> HcResult<Vec<Vec<Value>>> {
        let mut current = input;

        // Extract column names once for Lua steps.
        let col_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();

        for step in &self.steps {
            current = match step {
                TransformStep::Sql { sql } => {
                    let transform = SqlTransform::new(sql.clone());
                    transform.execute(db, columns, current).await?
                }
                TransformStep::Lua {
                    function,
                    inline,
                    script,
                    init,
                } => {
                    // Prefer inline Lua, then fall back to script file.
                    let transform = if let Some(script_path) = script.clone() {
                        LuaTransform::from_file(script_path, function.clone(), init.clone())?
                    } else {
                        // inline or neither — from_file expects a path; skip gracefully
                        return Err(crate::error::HcError::Transform(
                            "Lua transform requires a script path".into(),
                        ));
                    };
                    let _ = inline; // may be used in future
                                    // Use batch mode when the function name contains "batch",
                                    // otherwise fall back to per-message mode.
                    if function.contains("batch") {
                        transform.execute_batch(current, &col_names)?
                    } else {
                        transform.execute_per_message(current, &col_names)?
                    }
                }
            };
        }

        Ok(current)
    }
}

// src/transform/mod.rs
//
// TransformPipeline: runs a sequence of TransformSteps, feeding each step's
// output into the next.  All DuckDB access is channelled through DbManager.

pub mod lua;
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

        for step in &self.steps {
            current = match step {
                TransformStep::Sql { sql } => {
                    let transform = SqlTransform::new(sql.clone());
                    transform.execute(db, columns, current).await?
                }
                TransformStep::Lua {
                    script,
                    function,
                    init,
                } => {
                    // LuaTransform::new returns an Err immediately (stub).
                    let transform = LuaTransform::new(script, function, init.as_deref())?;
                    transform.execute(db, columns, current).await?
                }
            };
        }

        Ok(current)
    }
}

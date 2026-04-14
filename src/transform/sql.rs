// src/transform/sql.rs
//
// SQL transform step: loads input rows into a temp DuckDB table, executes
// user-provided SQL against it, and returns the resulting rows.

use serde_json::Value;

use crate::config::ColumnDef;
use crate::db_manager::DbManager;
use crate::error::HcResult;

pub struct SqlTransform {
    sql: String,
}

impl SqlTransform {
    pub fn new(sql: String) -> Self {
        SqlTransform { sql }
    }

    /// Execute the SQL transform.
    ///
    /// 1. Creates a temp table `raw_buffer` with the given column schema.
    /// 2. Inserts all input rows via a VALUES clause.
    /// 3. Runs the user SQL against `raw_buffer`.
    /// 4. Returns the resulting rows as `Vec<Vec<Value>>`.
    /// 5. Drops `raw_buffer` when done.
    pub async fn execute(
        &self,
        db: &DbManager,
        columns: &[ColumnDef],
        input: Vec<Vec<Value>>,
    ) -> HcResult<Vec<Vec<Value>>> {
        if input.is_empty() {
            return Ok(vec![]);
        }

        // --- 1. CREATE OR REPLACE TEMP TABLE raw_buffer ---
        let col_defs: Vec<String> = columns
            .iter()
            .map(|c| format!("{} {}", c.name, c.col_type))
            .collect();
        let create_sql = format!(
            "CREATE OR REPLACE TEMP TABLE raw_buffer ({})",
            col_defs.join(", ")
        );
        db.execute(&create_sql, vec![]).await?;

        // --- 2. INSERT rows via VALUES clause ---
        let value_rows: Vec<String> = input
            .iter()
            .map(|row| {
                let literals: Vec<String> = row
                    .iter()
                    .zip(columns.iter())
                    .map(|(v, col)| value_to_sql(v, &col.col_type))
                    .collect();
                format!("({})", literals.join(", "))
            })
            .collect();

        let insert_sql = format!("INSERT INTO raw_buffer VALUES {}", value_rows.join(", "));
        db.execute(&insert_sql, vec![]).await?;

        // --- 3. Execute user SQL ---
        let rows_maps = db.query_json(&self.sql, vec![]).await?;

        // --- 4. Convert Map rows → Vec<Vec<Value>> preserving column order ---
        // The result column order comes from the query; we return each row as
        // an ordered vec of values (same order as the map's iteration order,
        // which for serde_json::Map is insertion order == column order).
        let result: Vec<Vec<Value>> = rows_maps
            .into_iter()
            .map(|map| map.into_values().collect())
            .collect();

        // --- 5. DROP raw_buffer ---
        db.execute("DROP TABLE IF EXISTS raw_buffer", vec![])
            .await?;

        Ok(result)
    }
}

/// Convert a JSON `Value` into a SQL literal string suitable for embedding
/// directly in a VALUES clause.
pub fn value_to_sql(v: &Value, col_type: &str) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => {
            if *b {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        Value::Number(n) => {
            // Numbers are safe to embed as-is (no injection risk from JSON Number).
            n.to_string()
        }
        Value::String(s) => {
            // Escape single quotes by doubling them.
            let escaped = s.replace('\'', "''");
            // For timestamp/date types wrap in a cast so DuckDB parses them.
            let upper = col_type.to_uppercase();
            if upper.starts_with("TIMESTAMP") || upper == "DATE" {
                format!("'{}'::TIMESTAMP", escaped)
            } else {
                format!("'{}'", escaped)
            }
        }
        // Arrays / objects are serialised as JSON text.
        other => {
            let s = other.to_string().replace('\'', "''");
            format!("'{}'", s)
        }
    }
}

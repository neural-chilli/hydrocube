// src/ingest/parser.rs
use serde_json::Value;

use crate::config::ColumnDef;
use crate::error::{HcError, HcResult};

/// Parses raw JSON bytes into an ordered row of `serde_json::Value`s,
/// aligned to the cube schema column order.
pub struct JsonParser {
    column_names: Vec<String>,
}

impl JsonParser {
    /// Build a parser from the schema column definitions.
    pub fn new(columns: &[ColumnDef]) -> Self {
        Self {
            column_names: columns.iter().map(|c| c.name.clone()).collect(),
        }
    }

    /// Parse a single JSON message.
    ///
    /// - Fields present in the schema are placed at the corresponding index.
    /// - Missing fields become `Value::Null`.
    /// - Extra fields not in the schema are ignored.
    /// - Malformed JSON returns `HcError::Ingest`.
    pub fn parse(&self, data: &[u8]) -> HcResult<Vec<Value>> {
        let obj: serde_json::Map<String, Value> =
            serde_json::from_slice(data).map_err(|e| HcError::Ingest(e.to_string()))?;

        let row = self
            .column_names
            .iter()
            .map(|name| obj.get(name).cloned().unwrap_or(Value::Null))
            .collect();

        Ok(row)
    }
}

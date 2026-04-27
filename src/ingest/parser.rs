// src/ingest/parser.rs
use serde_json::Value;

use crate::config::{ColumnDef, DataFormat, TableConfig};
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

/// Parse raw bytes into one or more rows according to `format`.
///
/// Returns `Vec<Vec<Value>>` so that formats like CSV or JSON Lines can yield
/// multiple rows per message. JSON always returns exactly one row.
///
/// `_decoders` is a placeholder for the Avro/Protobuf decoder map wired in Task 4.
pub fn parse_rows_for_table(
    bytes: &[u8],
    format: &DataFormat,
    table_cfg: &TableConfig,
    _decoders: Option<&()>,
) -> HcResult<Vec<Vec<Value>>> {
    match format {
        DataFormat::Json | DataFormat::JsonLines => {
            let parser = JsonParser::new(&table_cfg.schema.columns);
            parser.parse(bytes).map(|row| vec![row])
        }
        _ => Err(HcError::Ingest(format!(
            "format {:?} not yet supported for streaming messages",
            format
        ))),
    }
}

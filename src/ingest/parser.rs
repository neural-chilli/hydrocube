// src/ingest/parser.rs
use serde_json::Value;

use crate::config::{ColumnDef, DataFormat, TableConfig};
use crate::error::{HcError, HcResult};

pub struct JsonParser {
    column_names: Vec<String>,
}

impl JsonParser {
    pub fn new(columns: &[ColumnDef]) -> Self {
        Self {
            column_names: columns.iter().map(|c| c.name.clone()).collect(),
        }
    }

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
        DataFormat::Json => {
            let parser = JsonParser::new(&table_cfg.schema.columns);
            parser.parse(bytes).map(|row| vec![row])
        }
        DataFormat::JsonLines => {
            let parser = JsonParser::new(&table_cfg.schema.columns);
            let mut rows = Vec::new();
            for line in bytes.split(|&b| b == b'\n') {
                if line.is_empty() {
                    continue;
                }
                let row = parser.parse(line)?;
                rows.push(row);
            }
            Ok(rows)
        }
        _ => Err(HcError::Ingest(format!(
            "format {:?} not yet supported for streaming messages",
            format
        ))),
    }
}

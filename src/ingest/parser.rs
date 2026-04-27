// src/ingest/parser.rs
use serde_json::Value;

use crate::config::{ColumnDef, DataFormat, TableConfig};
use crate::error::{HcError, HcResult};
use crate::ingest::{CompiledDecoder, CompiledDecoderMap};

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
pub fn parse_rows_for_table(
    bytes: &[u8],
    format: &DataFormat,
    table_cfg: &TableConfig,
    decoders: Option<&CompiledDecoderMap>,
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
        DataFormat::Csv => {
            let parser = CsvParser::new(&table_cfg.schema.columns);
            parser.parse(bytes)
        }
        DataFormat::Avro => {
            if let Some(map) = decoders {
                if let Some(CompiledDecoder::Avro(dec)) = map.get(&table_cfg.name) {
                    return dec.decode(bytes);
                }
            }
            Err(HcError::Ingest(format!(
                "no Avro decoder compiled for table '{}'",
                table_cfg.name
            )))
        }
        DataFormat::Protobuf => {
            if let Some(map) = decoders {
                if let Some(CompiledDecoder::Protobuf(dec)) = map.get(&table_cfg.name) {
                    return dec.decode(bytes);
                }
            }
            Err(HcError::Ingest(format!(
                "no Protobuf decoder compiled for table '{}'",
                table_cfg.name
            )))
        }
        _ => Err(HcError::Ingest(format!(
            "format {:?} not yet supported for streaming messages",
            format
        ))),
    }
}

pub struct CsvParser {
    column_names: Vec<String>,
}

impl CsvParser {
    pub fn new(columns: &[ColumnDef]) -> Self {
        Self {
            column_names: columns.iter().map(|c| c.name.clone()).collect(),
        }
    }

    pub fn parse(&self, data: &[u8]) -> HcResult<Vec<Vec<Value>>> {
        let mut rdr = csv::Reader::from_reader(data);
        let headers = rdr
            .headers()
            .map_err(|e| HcError::Ingest(e.to_string()))?
            .clone();

        let header_index: std::collections::HashMap<&str, usize> =
            headers.iter().enumerate().map(|(i, h)| (h, i)).collect();

        let mut rows = Vec::new();
        for result in rdr.records() {
            let record = result.map_err(|e| HcError::Ingest(e.to_string()))?;
            let row = self
                .column_names
                .iter()
                .map(|col_name| {
                    header_index
                        .get(col_name.as_str())
                        .and_then(|&idx| record.get(idx))
                        .map(|s| Value::String(s.to_owned()))
                        .unwrap_or(Value::Null)
                })
                .collect();
            rows.push(row);
        }
        Ok(rows)
    }
}

// src/ingest/avro.rs
use apache_avro::{from_avro_datum, types::Value as AvroValue, Schema};
use serde_json::Value;

use crate::config::ColumnDef;
use crate::error::{HcError, HcResult};

pub struct AvroDecoder {
    schema: Schema,
    column_names: Vec<String>,
}

impl AvroDecoder {
    pub fn new(schema_json: &str, columns: &[ColumnDef]) -> HcResult<Self> {
        let schema = Schema::parse_str(schema_json)
            .map_err(|e| HcError::Config(format!("invalid Avro schema: {e}")))?;
        Ok(Self {
            schema,
            column_names: columns.iter().map(|c| c.name.clone()).collect(),
        })
    }

    pub fn decode(&self, bytes: &[u8]) -> HcResult<Vec<Vec<Value>>> {
        let mut cursor = std::io::Cursor::new(bytes);
        let avro_val = from_avro_datum(&self.schema, &mut cursor, None)
            .map_err(|e| HcError::Ingest(format!("Avro decode error: {e}")))?;

        let row = match avro_val {
            AvroValue::Record(fields) => {
                let field_map: std::collections::HashMap<String, AvroValue> =
                    fields.into_iter().collect();
                self.column_names
                    .iter()
                    .map(|col| {
                        field_map
                            .get(col)
                            .map(avro_val_to_json)
                            .unwrap_or(Value::Null)
                    })
                    .collect()
            }
            _ => return Err(HcError::Ingest("Avro message must be a Record".into())),
        };
        Ok(vec![row])
    }
}

fn avro_val_to_json(val: &AvroValue) -> Value {
    match val {
        AvroValue::Null => Value::Null,
        AvroValue::Boolean(b) => Value::Bool(*b),
        AvroValue::Int(i) => Value::from(*i),
        AvroValue::Long(l) => Value::from(*l),
        AvroValue::Float(f) => Value::from(*f as f64),
        AvroValue::Double(d) => Value::from(*d),
        AvroValue::String(s) | AvroValue::Enum(_, s) => Value::String(s.clone()),
        AvroValue::Union(_, inner) => avro_val_to_json(inner),
        _ => Value::Null,
    }
}

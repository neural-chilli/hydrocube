use base64::Engine;
use protobuf::reflect::ReflectValueRef;
use serde_json::Value;

use crate::config::ColumnDef;
use crate::error::{HcError, HcResult};

#[derive(Debug)]
pub struct ProtobufDecoder {
    descriptor: protobuf::reflect::MessageDescriptor,
    column_names: Vec<String>,
}

impl ProtobufDecoder {
    pub fn new(proto_schema: &str, message_type: &str, columns: &[ColumnDef]) -> HcResult<Self> {
        let tmp_dir = tempfile::tempdir()
            .map_err(|e| HcError::Config(format!("temp dir for proto schema: {e}")))?;
        let proto_path = tmp_dir.path().join("schema.proto");
        std::fs::write(&proto_path, proto_schema)
            .map_err(|e| HcError::Config(format!("write proto schema: {e}")))?;

        let parsed = protobuf_parse::Parser::new()
            .pure()
            .include(tmp_dir.path())
            .input(&proto_path)
            .parse_and_typecheck()
            .map_err(|e| HcError::Config(format!("invalid proto schema: {e}")))?;

        let fdp =
            parsed.file_descriptors.into_iter().next().ok_or_else(|| {
                HcError::Config("proto schema produced no file descriptors".into())
            })?;

        let file_descriptor = protobuf::reflect::FileDescriptor::new_dynamic(fdp, &[])
            .map_err(|e| HcError::Config(format!("proto reflection error: {e}")))?;

        let descriptor = file_descriptor
            .message_by_package_relative_name(message_type)
            .ok_or_else(|| {
                HcError::Config(format!(
                    "proto message type '{message_type}' not found in schema"
                ))
            })?;

        Ok(Self {
            descriptor,
            column_names: columns.iter().map(|c| c.name.clone()).collect(),
        })
    }

    pub fn decode(&self, bytes: &[u8]) -> HcResult<Vec<Vec<Value>>> {
        let msg = self
            .descriptor
            .parse_from_bytes(bytes)
            .map_err(|e| HcError::Ingest(format!("Protobuf decode error: {e}")))?;

        let row = self
            .column_names
            .iter()
            .map(|col_name| {
                self.descriptor
                    .field_by_name(col_name)
                    .map(|f| proto_reflect_to_json(f.get_singular_field_or_default(msg.as_ref())))
                    .unwrap_or(Value::Null)
            })
            .collect();

        Ok(vec![row])
    }
}

fn proto_reflect_to_json(val: ReflectValueRef) -> Value {
    match val {
        ReflectValueRef::Bool(b) => Value::Bool(b),
        ReflectValueRef::U32(n) => Value::from(n),
        ReflectValueRef::U64(n) => Value::from(n),
        ReflectValueRef::I32(n) => Value::from(n),
        ReflectValueRef::I64(n) => Value::from(n),
        ReflectValueRef::F32(f) => Value::from(f as f64),
        ReflectValueRef::F64(f) => Value::from(f),
        ReflectValueRef::String(s) => Value::String(s.to_owned()),
        ReflectValueRef::Bytes(b) => {
            Value::String(base64::engine::general_purpose::STANDARD.encode(b))
        }
        ReflectValueRef::Enum(d, n) => Value::String(
            d.value_by_number(n)
                .map(|v| v.name().to_owned())
                .unwrap_or_else(|| n.to_string()),
        ),
        ReflectValueRef::Message(_) => Value::Null,
    }
}

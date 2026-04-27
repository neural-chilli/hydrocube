use hydrocube::config::ColumnDef;
use hydrocube::ingest::protobuf::ProtobufDecoder;
use serde_json::Value;

fn col(name: &str) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        col_type: "VARCHAR".into(),
    }
}

fn encode_rate_update(curve: &str, rate: f64) -> Vec<u8> {
    let mut buf = Vec::new();
    let curve_bytes = curve.as_bytes();
    buf.push(0x0A);
    buf.push(curve_bytes.len() as u8);
    buf.extend_from_slice(curve_bytes);
    buf.push(0x11);
    buf.extend_from_slice(&rate.to_le_bytes());
    buf
}

#[test]
fn test_protobuf_decoder_round_trip() {
    let proto_schema = r#"
syntax = "proto3";
message RateUpdate {
    string curve = 1;
    double rate = 2;
}
"#;
    let columns = vec![col("curve"), col("rate")];
    let decoder =
        ProtobufDecoder::new(proto_schema, "RateUpdate", &columns).expect("schema compiles");

    let bytes = encode_rate_update("USD.3M", 0.0525);
    let rows = decoder.decode(&bytes).expect("decode");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::String("USD.3M".into()));
    assert!(
        rows[0][1].is_number(),
        "rate should be a number, got {:?}",
        rows[0][1]
    );
}

#[test]
fn test_protobuf_decoder_invalid_schema_returns_error() {
    let result = ProtobufDecoder::new("not proto", "Msg", &[]);
    assert!(result.is_err());
}

#[test]
fn test_protobuf_decoder_unknown_message_type_returns_error() {
    let proto_schema = r#"
syntax = "proto3";
message RateUpdate { string curve = 1; }
"#;
    let result = ProtobufDecoder::new(proto_schema, "NonExistent", &[col("curve")]);
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("NonExistent"),
        "expected message type error, got: {msg}"
    );
}

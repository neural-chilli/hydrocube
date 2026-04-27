// tests/avro_decoder_test.rs
use hydrocube::config::ColumnDef;
use hydrocube::ingest::avro::AvroDecoder;
use serde_json::Value;

fn col(name: &str) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        col_type: "VARCHAR".into(),
    }
}

fn encode_avro_datum(schema_json: &str, fields: Vec<(&str, apache_avro::types::Value)>) -> Vec<u8> {
    let schema = apache_avro::Schema::parse_str(schema_json).unwrap();
    let record = apache_avro::types::Value::Record(
        fields.into_iter().map(|(k, v)| (k.to_owned(), v)).collect(),
    );
    apache_avro::to_avro_datum(&schema, record).unwrap()
}

#[test]
fn test_avro_decoder_round_trip() {
    let schema_json = r#"{"type":"record","name":"Trade","fields":[
        {"name":"trade_id","type":"string"},
        {"name":"notional","type":"double"}
    ]}"#;
    let columns = vec![col("trade_id"), col("notional")];
    let decoder = AvroDecoder::new(schema_json, &columns).expect("schema compiles");

    let bytes = encode_avro_datum(
        schema_json,
        vec![
            ("trade_id", apache_avro::types::Value::String("T001".into())),
            ("notional", apache_avro::types::Value::Double(1_000_000.0)),
        ],
    );

    let rows = decoder.decode(&bytes).expect("decode");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::String("T001".into()));
    assert_eq!(rows[0][1], Value::from(1_000_000.0_f64));
}

#[test]
fn test_avro_decoder_missing_field_becomes_null() {
    let schema_json = r#"{"type":"record","name":"T","fields":[
        {"name":"a","type":"string"}
    ]}"#;
    let columns = vec![col("a"), col("b")];
    let decoder = AvroDecoder::new(schema_json, &columns).expect("compiles");

    let bytes = encode_avro_datum(
        schema_json,
        vec![("a", apache_avro::types::Value::String("v".into()))],
    );
    let rows = decoder.decode(&bytes).expect("decode");
    assert_eq!(rows[0][1], Value::Null);
}

#[test]
fn test_avro_decoder_invalid_schema_returns_error() {
    let result = AvroDecoder::new("not valid json", &[]);
    assert!(result.is_err());
}

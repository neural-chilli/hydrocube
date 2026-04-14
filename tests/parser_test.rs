// tests/parser_test.rs
use hydrocube::config::ColumnDef;
use hydrocube::ingest::parser::JsonParser;
use serde_json::Value;

fn make_column(name: &str) -> ColumnDef {
    ColumnDef {
        name: name.to_string(),
        col_type: "string".to_string(),
    }
}

#[test]
fn test_parse_json_message() {
    let columns = vec![
        make_column("sensor_id"),
        make_column("temperature"),
        make_column("ts"),
    ];
    let parser = JsonParser::new(&columns);

    let msg = br#"{"sensor_id": "s1", "temperature": 42.5, "ts": "2024-01-01T00:00:00Z"}"#;
    let result = parser.parse(msg).expect("parse should succeed");

    assert_eq!(result.len(), 3);
    assert_eq!(result[0], Value::String("s1".to_string()));
    assert_eq!(result[1], Value::from(42.5_f64));
    assert_eq!(result[2], Value::String("2024-01-01T00:00:00Z".to_string()));
}

#[test]
fn test_parse_json_missing_field_becomes_null() {
    let columns = vec![
        make_column("sensor_id"),
        make_column("temperature"),
        make_column("missing_field"),
    ];
    let parser = JsonParser::new(&columns);

    let msg = br#"{"sensor_id": "s2", "temperature": 99.9}"#;
    let result = parser.parse(msg).expect("parse should succeed");

    assert_eq!(result.len(), 3);
    assert_eq!(result[0], Value::String("s2".to_string()));
    assert_eq!(result[1], Value::from(99.9_f64));
    assert_eq!(result[2], Value::Null);
}

#[test]
fn test_parse_json_extra_fields_ignored() {
    let columns = vec![make_column("sensor_id"), make_column("temperature")];
    let parser = JsonParser::new(&columns);

    let msg =
        br#"{"sensor_id": "s3", "temperature": 55.0, "extra_field": "ignored", "another": 123}"#;
    let result = parser.parse(msg).expect("parse should succeed");

    // Only columns defined in schema are returned
    assert_eq!(result.len(), 2);
    assert_eq!(result[0], Value::String("s3".to_string()));
    assert_eq!(result[1], Value::from(55.0_f64));
}

#[test]
fn test_parse_json_malformed_returns_error() {
    let columns = vec![make_column("sensor_id")];
    let parser = JsonParser::new(&columns);

    let msg = b"not valid json {{{";
    let result = parser.parse(msg);

    assert!(result.is_err(), "malformed JSON should return an error");
    let err = result.unwrap_err();
    let err_str = err.to_string();
    assert!(
        err_str.contains("ingest error"),
        "error should be an ingest error, got: {}",
        err_str
    );
}

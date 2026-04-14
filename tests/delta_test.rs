// tests/delta_test.rs
//
// Unit tests for the DeltaDetector.  All RecordBatch construction uses
// duckdb::arrow types to match the types returned by db_manager::query_arrow().

use std::sync::Arc;

use duckdb::arrow::array::{Float64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;

use hydrocube::delta::DeltaDetector;

// ---------------------------------------------------------------------------
// Helper: build a RecordBatch with "book" (Utf8) and "total_notional" (Float64)
// ---------------------------------------------------------------------------

fn make_batch(books: &[&str], totals: &[f64]) -> RecordBatch {
    assert_eq!(
        books.len(),
        totals.len(),
        "books and totals must have the same length"
    );

    let schema = Arc::new(Schema::new(vec![
        Field::new("book", DataType::Utf8, false),
        Field::new("total_notional", DataType::Float64, false),
    ]));

    let book_array = Arc::new(StringArray::from(books.to_vec()));
    let total_array = Arc::new(Float64Array::from(totals.to_vec()));

    RecordBatch::try_new(schema, vec![book_array, total_array])
        .expect("make_batch: failed to create RecordBatch")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn test_first_window_all_rows_are_deltas() {
    let mut detector = DeltaDetector::new(vec!["book".into()]);

    let batch = make_batch(&["FX", "Rates", "Credit"], &[100.0, 200.0, 50.0]);
    let (upserts, deletes) = detector.detect(&batch);

    // First window: all rows are new, so all 3 should be upserts.
    assert_eq!(
        upserts.num_rows(),
        3,
        "first window: expected 3 upserts (all new), got {}",
        upserts.num_rows()
    );
    assert_eq!(
        deletes.num_rows(),
        0,
        "first window: expected 0 deletes, got {}",
        deletes.num_rows()
    );
}

#[test]
fn test_unchanged_rows_not_in_delta() {
    let mut detector = DeltaDetector::new(vec!["book".into()]);

    let batch = make_batch(&["FX", "Rates"], &[100.0, 200.0]);

    // First window: prime state.
    let _ = detector.detect(&batch);

    // Second window: identical data — no changes.
    let same_batch = make_batch(&["FX", "Rates"], &[100.0, 200.0]);
    let (upserts, deletes) = detector.detect(&same_batch);

    assert_eq!(
        upserts.num_rows(),
        0,
        "unchanged rows: expected 0 upserts, got {}",
        upserts.num_rows()
    );
    assert_eq!(
        deletes.num_rows(),
        0,
        "unchanged rows: expected 0 deletes, got {}",
        deletes.num_rows()
    );
}

#[test]
fn test_changed_row_detected() {
    let mut detector = DeltaDetector::new(vec!["book".into()]);

    // Window 1.
    let batch1 = make_batch(&["FX", "Rates"], &[100.0, 200.0]);
    let _ = detector.detect(&batch1);

    // Window 2: FX total_notional changed; Rates unchanged.
    let batch2 = make_batch(&["FX", "Rates"], &[150.0, 200.0]);
    let (upserts, deletes) = detector.detect(&batch2);

    assert_eq!(
        upserts.num_rows(),
        1,
        "changed row: expected 1 upsert (FX changed), got {}",
        upserts.num_rows()
    );
    assert_eq!(
        deletes.num_rows(),
        0,
        "changed row: expected 0 deletes, got {}",
        deletes.num_rows()
    );

    // Verify the upserted row is FX.
    let book_col = upserts
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("book column must be StringArray");
    assert_eq!(book_col.value(0), "FX", "upserted row should be FX");
}

#[test]
fn test_deleted_group_detected() {
    let mut detector = DeltaDetector::new(vec!["book".into()]);

    // Window 1: FX and Rates present.
    let batch1 = make_batch(&["FX", "Rates"], &[100.0, 200.0]);
    let _ = detector.detect(&batch1);

    // Window 2: Rates disappears.
    let batch2 = make_batch(&["FX"], &[100.0]);
    let (upserts, deletes) = detector.detect(&batch2);

    // FX unchanged → 0 upserts.
    assert_eq!(
        upserts.num_rows(),
        0,
        "deleted group: expected 0 upserts (FX unchanged), got {}",
        upserts.num_rows()
    );

    // Rates gone → 1 delete.
    assert_eq!(
        deletes.num_rows(),
        1,
        "deleted group: expected 1 delete (Rates gone), got {}",
        deletes.num_rows()
    );
}

#[test]
fn test_new_group_detected() {
    let mut detector = DeltaDetector::new(vec!["book".into()]);

    // Window 1: only FX.
    let batch1 = make_batch(&["FX"], &[100.0]);
    let _ = detector.detect(&batch1);

    // Window 2: FX unchanged, Credit appears.
    let batch2 = make_batch(&["FX", "Credit"], &[100.0, 75.0]);
    let (upserts, deletes) = detector.detect(&batch2);

    // Credit is new → 1 upsert.
    assert_eq!(
        upserts.num_rows(),
        1,
        "new group: expected 1 upsert (Credit new), got {}",
        upserts.num_rows()
    );
    assert_eq!(
        deletes.num_rows(),
        0,
        "new group: expected 0 deletes, got {}",
        deletes.num_rows()
    );

    // Verify the upserted row is Credit.
    let book_col = upserts
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("book column must be StringArray");
    assert_eq!(book_col.value(0), "Credit", "upserted row should be Credit");
}

#[test]
fn test_empty_batch_clears_state() {
    let mut detector = DeltaDetector::new(vec!["book".into()]);

    // Window 1: some data.
    let batch1 = make_batch(&["FX", "Rates"], &[100.0, 200.0]);
    let _ = detector.detect(&batch1);

    // Window 2: empty batch — all previous groups become deletes.
    let empty = make_batch(&[], &[]);
    let (upserts, deletes) = detector.detect(&empty);

    assert_eq!(upserts.num_rows(), 0, "empty window: expected 0 upserts");
    assert_eq!(
        deletes.num_rows(),
        2,
        "empty window: expected 2 deletes (FX + Rates), got {}",
        deletes.num_rows()
    );
}

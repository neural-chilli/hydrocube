// src/delta.rs
//
// Delta detection for aggregate RecordBatches.
//
// Each window produces a RecordBatch of aggregate rows.  DeltaDetector compares
// the current batch against the previous window's state (stored as a HashMap of
// dimension_key → measure values) and returns two filtered batches:
//   - upserts: rows that are new or have changed measure values
//   - deletes: rows that were present in the previous window but are gone now
//
// When epsilon > 0.0, float measure changes smaller than epsilon are suppressed
// (treated as unchanged) to avoid spurious updates from floating-point noise.

use std::collections::HashMap;
use std::sync::Arc;

use duckdb::arrow::array::{
    Array, ArrayRef, Float64Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt32Array,
};
use duckdb::arrow::datatypes::{DataType, TimeUnit};
use duckdb::arrow::record_batch::RecordBatch;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Detects changed rows between successive aggregate windows.
pub struct DeltaDetector {
    /// Names of dimension columns (used to build the row key).
    dimension_names: Vec<String>,
    /// Previous window state: dimension_key → stringified measure values.
    previous: HashMap<String, Vec<String>>,
    /// Suppress float changes smaller than this threshold (0.0 = exact comparison).
    epsilon: f64,
}

impl DeltaDetector {
    /// Create a new detector with empty previous state.
    ///
    /// `epsilon` controls float suppression: changes where `|prev - curr| <= epsilon`
    /// are treated as unchanged.  Pass `0.0` to keep the exact-comparison behaviour.
    pub fn new(dimension_names: Vec<String>, epsilon: f64) -> Self {
        Self {
            dimension_names,
            previous: HashMap::new(),
            epsilon,
        }
    }

    /// Reset all previous state. After this call the next `detect` treats all
    /// rows as new (no prior window to compare against).
    pub fn clear(&mut self) {
        self.previous.clear();
    }

    /// Compare `current` against the previous window.
    ///
    /// Returns `(upserts, deletes)`:
    /// - `upserts` contains rows that are new or have changed measures.
    /// - `deletes` contains rows whose dimension keys were in the previous window
    ///   but are absent from `current`.
    pub fn detect(&mut self, current: &RecordBatch) -> (RecordBatch, RecordBatch) {
        let schema = current.schema();

        // Resolve column indices for dimensions and measures.
        let dim_indices: Vec<usize> = self
            .dimension_names
            .iter()
            .filter_map(|name| schema.index_of(name).ok())
            .collect();

        let measure_indices: Vec<usize> = (0..schema.fields().len())
            .filter(|i| {
                let name = schema.field(*i).name();
                !self.dimension_names.contains(name)
            })
            .collect();

        // Build current-window state.
        let mut current_state: HashMap<String, Vec<String>> = HashMap::new();
        let mut upsert_indices: Vec<u32> = Vec::new();

        for row in 0..current.num_rows() {
            let key = build_dimension_key(current, &dim_indices, row);
            let measure_vals: Vec<String> = measure_indices
                .iter()
                .map(|&col| array_value_to_string(current.column(col), row))
                .collect();

            current_state.insert(key.clone(), measure_vals.clone());

            let changed = match self.previous.get(&key) {
                None => true, // new group
                Some(prev_vals) => self.values_changed(prev_vals, &measure_vals),
            };

            if changed {
                upsert_indices.push(row as u32);
            }
        }

        // Keys present in previous but absent in current → deletes.
        let delete_keys: Vec<&String> = self
            .previous
            .keys()
            .filter(|k| !current_state.contains_key(*k))
            .collect();

        // Build delete batch by finding previous-window rows that correspond to
        // the deleted keys.  Since we only store hashes (not row data), we build
        // a minimal batch containing only the dimension columns set to the key
        // values.  However, to keep the contract simple and the schema consistent,
        // we record which rows in the *current* batch have gone away — but since
        // they don't appear in current, we return an empty-schema batch tagged
        // with dimension-key strings instead.
        //
        // For the actual implementation we return a RecordBatch that has the same
        // schema as `current` but with zero rows for deletes that no longer
        // appear.  The caller can inspect `delete_keys` via the returned batch's
        // row count.  A zero-row batch with the right schema is idiomatic.
        //
        // Practical approach: build an empty-row RecordBatch for deletes (the
        // dimension key is tracked by the caller via the previous state).  If
        // there are deleted groups, we return a batch whose columns are
        // reconstructed from key strings — but this requires reversing the key
        // encoding which is complex and lossy (only VARCHAR dimensions survive).
        //
        // Simpler and lossless: return a batch with only the rows that were
        // deleted (not present in current).  Since those rows aren't in current,
        // we return a zero-row batch tagged with the current schema.  The count
        // of deletes is conveyed by `delete_keys.len()`.
        //
        // For a clean API, we return a 0-row RecordBatch for deletes (or build
        // a synthetic batch from the key strings when all dimensions are strings).

        let delete_batch = build_delete_batch(current, &delete_keys, &self.dimension_names);

        // Update stored state.
        self.previous = current_state;

        let upsert_batch = filter_batch(current, &upsert_indices);

        (upsert_batch, delete_batch)
    }

    /// Return the number of groups tracked from the previous window.
    pub fn previous_count(&self) -> usize {
        self.previous.len()
    }

    /// Compare two measure-value vectors, applying epsilon suppression for floats.
    ///
    /// Returns `true` if the values are considered changed.
    fn values_changed(&self, prev: &[String], curr: &[String]) -> bool {
        if prev.len() != curr.len() {
            return true;
        }
        for (p, c) in prev.iter().zip(curr.iter()) {
            if p == c {
                continue; // exact string match — no change
            }
            // Try epsilon comparison for floats when epsilon > 0.
            if self.epsilon > 0.0 {
                if let (Ok(pf), Ok(cf)) = (p.parse::<f64>(), c.parse::<f64>()) {
                    if (pf - cf).abs() <= self.epsilon {
                        continue; // within epsilon — suppress
                    }
                    return true;
                }
            }
            // Non-float or epsilon == 0: any string difference counts.
            return true;
        }
        false
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Build a string key from the dimension column values of a single row.
/// Values are concatenated with "|" as separator.
fn build_dimension_key(batch: &RecordBatch, dim_indices: &[usize], row: usize) -> String {
    dim_indices
        .iter()
        .map(|&col| array_value_to_string(batch.column(col), row))
        .collect::<Vec<_>>()
        .join("|")
}

/// Convert a single value from an Arrow array to a String for comparison/key-building.
/// Handles the common DuckDB output types.  Falls back to `"<null>"` for nulls.
///
/// Float columns are formatted with full precision so that epsilon comparison
/// can later parse them back to f64 exactly.
/// Handles the common DuckDB output types.  Falls back to `"<null>"` for nulls.
fn array_value_to_string(array: &ArrayRef, row: usize) -> String {
    if array.is_null(row) {
        return "<null>".to_owned();
    }

    match array.data_type() {
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            arr.value(row).to_owned()
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            // Full-precision decimal representation so epsilon comparison can
            // parse the stored string back to f64 without loss.
            format!("{}", arr.value(row))
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::UInt64 => {
            let arr = array
                .as_any()
                .downcast_ref::<duckdb::arrow::array::UInt64Array>()
                .unwrap();
            arr.value(row).to_string()
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<duckdb::arrow::array::Float32Array>()
                .unwrap();
            format!("{}", arr.value(row))
        }
        // Timestamps: read the raw integer value so changes are detectable.
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            arr.value(row).to_string()
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            arr.value(row).to_string()
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap();
            arr.value(row).to_string()
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            arr.value(row).to_string()
        }
        // Dates and other types: use a stable debug representation (excludes row index).
        other => format!("{:?}", other),
    }
}

/// Extract specific rows by index, returning a new RecordBatch.
/// Uses `duckdb::arrow::compute::take` internally.
fn filter_batch(batch: &RecordBatch, indices: &[u32]) -> RecordBatch {
    if indices.is_empty() {
        return RecordBatch::new_empty(batch.schema());
    }

    // Build an Int32 / UInt32 index array for `take`.
    let index_array = Arc::new(UInt32Array::from(indices.to_vec())) as ArrayRef;

    let new_columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|col| {
            duckdb::arrow::compute::take(col.as_ref(), index_array.as_ref(), None)
                .expect("take failed")
        })
        .collect();

    RecordBatch::try_new(batch.schema(), new_columns).expect("filter_batch: schema mismatch")
}

/// Build a RecordBatch representing deleted groups.
///
/// Since deleted rows are no longer in `current`, we reconstruct them from the
/// stored dimension keys.  We only reconstruct dimension columns; measure
/// columns are set to null.  This produces a batch with the same schema as
/// `current` but with null measures — sufficient for consumers to identify
/// which dimension groups have been removed.
fn build_delete_batch(
    current: &RecordBatch,
    delete_keys: &[&String],
    dimension_names: &[String],
) -> RecordBatch {
    if delete_keys.is_empty() {
        return RecordBatch::new_empty(current.schema());
    }

    use duckdb::arrow::array::{new_null_array, StringBuilder};
    use duckdb::arrow::datatypes::{DataType, Field, Schema};

    let orig_schema = current.schema();
    let n = delete_keys.len();

    // Build a nullable version of the schema so we can fill measure columns
    // with nulls without violating non-null constraints from the source schema.
    let nullable_fields: Vec<Field> = orig_schema
        .fields()
        .iter()
        .map(|f| Field::new(f.name(), f.data_type().clone(), true))
        .collect();
    let nullable_schema = Arc::new(Schema::new(nullable_fields));

    // Parse each key back into per-dimension values.
    let key_parts: Vec<Vec<&str>> = delete_keys.iter().map(|k| k.split('|').collect()).collect();

    let columns: Vec<ArrayRef> = (0..nullable_schema.fields().len())
        .map(|col_idx| {
            let field = nullable_schema.field(col_idx);
            let dim_pos = dimension_names.iter().position(|d| d == field.name());

            if let Some(pos) = dim_pos {
                // Reconstruct the dimension column from key parts.
                // Only VARCHAR dimensions can be losslessly reconstructed.
                match field.data_type() {
                    DataType::Utf8 => {
                        let mut builder = StringBuilder::with_capacity(n, n * 8);
                        for parts in &key_parts {
                            let val = parts.get(pos).copied().unwrap_or("<null>");
                            if val == "<null>" {
                                builder.append_null();
                            } else {
                                builder.append_value(val);
                            }
                        }
                        Arc::new(builder.finish()) as ArrayRef
                    }
                    // For non-string dimension types, fall back to null.
                    dt => new_null_array(dt, n),
                }
            } else {
                // Measure column — null for delete records.
                new_null_array(field.data_type(), n)
            }
        })
        .collect();

    RecordBatch::try_new(nullable_schema, columns).expect("build_delete_batch: schema mismatch")
}

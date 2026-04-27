// src/tables/mod.rs
use serde_json::Value;
use std::collections::HashMap;

/// Buffer for an append-mode table: rows accumulate until the next window flush.
#[derive(Debug, Default)]
pub struct AppendBuffer {
    rows: Vec<Vec<Value>>,
}

impl AppendBuffer {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn push(&mut self, row: Vec<Value>) {
        self.rows.push(row);
    }
    pub fn len(&self) -> usize {
        self.rows.len()
    }
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
    /// Drain all rows, resetting the buffer.
    pub fn drain(&mut self) -> Vec<Vec<Value>> {
        std::mem::take(&mut self.rows)
    }
}

/// Buffer for a replace-mode (LVC) table: last writer per key wins.
/// Rows are stored as column-aligned Vec<Value> matching the table schema.
#[derive(Debug)]
pub struct ReplaceBuffer {
    map: HashMap<String, Vec<Value>>,
    key_indices: Vec<usize>,
    key_columns: Vec<String>,
    schema_columns: Vec<String>,
}

impl ReplaceBuffer {
    pub fn new(key_columns: Vec<String>, schema_columns: Vec<String>) -> Self {
        let key_indices = key_columns
            .iter()
            .map(|k| {
                schema_columns
                    .iter()
                    .position(|c| c == k)
                    .unwrap_or_else(|| panic!("key column '{k}' not found in schema"))
            })
            .collect();
        Self {
            map: HashMap::new(),
            key_indices,
            key_columns,
            schema_columns,
        }
    }

    /// Upsert a row (column-aligned to schema_columns). Last write wins per key.
    pub fn upsert(&mut self, row: Vec<Value>) {
        let key = self.make_key(&row);
        self.map.insert(key, row);
    }

    /// Flush all surviving rows and clear the map.
    pub fn flush(&mut self) -> Vec<Vec<Value>> {
        std::mem::take(&mut self.map).into_values().collect()
    }

    fn make_key(&self, row: &[Value]) -> String {
        self.key_indices
            .iter()
            .map(|&i| row.get(i).map(|v| v.to_string()).unwrap_or_default())
            .collect::<Vec<_>>()
            .join("|")
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
    pub fn key_columns(&self) -> &[String] {
        &self.key_columns
    }
    pub fn schema_columns(&self) -> &[String] {
        &self.schema_columns
    }
}

/// Unified buffer type per table.
#[derive(Debug)]
pub enum TableBuffer {
    Append(AppendBuffer),
    Replace(ReplaceBuffer),
}

impl TableBuffer {
    pub fn new_append() -> Self {
        Self::Append(AppendBuffer::new())
    }

    pub fn new_replace(key_columns: Vec<String>, schema_columns: Vec<String>) -> Self {
        Self::Replace(ReplaceBuffer::new(key_columns, schema_columns))
    }

    pub fn push_append(&mut self, row: Vec<Value>) {
        if let Self::Append(buf) = self {
            buf.push(row);
        }
    }

    pub fn upsert_replace(&mut self, row: Vec<Value>) {
        if let Self::Replace(buf) = self {
            buf.upsert(row);
        }
    }

    pub fn append_len(&self) -> usize {
        if let Self::Append(buf) = self {
            buf.len()
        } else {
            0
        }
    }

    pub fn replace_len(&self) -> usize {
        if let Self::Replace(buf) = self {
            buf.len()
        } else {
            0
        }
    }

    pub fn is_append(&self) -> bool {
        matches!(self, Self::Append(_))
    }
    pub fn is_replace(&self) -> bool {
        matches!(self, Self::Replace(_))
    }
}

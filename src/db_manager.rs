// src/db_manager.rs
//
// All DuckDB access is funnelled through a single std::thread that owns the
// one `duckdb::Connection`.  Callers communicate via a `tokio::mpsc` channel
// and receive results via `tokio::sync::oneshot` senders.

use std::path::Path;

use duckdb::{params_from_iter, Connection};
use serde_json::{Map, Value as JsonValue};
use tokio::sync::{mpsc, oneshot};

use crate::error::{HcError, HcResult};

// The arrow types exported by the duckdb crate (arrow 58).  We intentionally
// use `duckdb::arrow` here so we never have a cross-version type mismatch.
pub use duckdb::arrow::record_batch::RecordBatch;

// ---------------------------------------------------------------------------
// Channel message types
// ---------------------------------------------------------------------------

/// Commands sent to the DB manager thread.
enum DbCommand {
    /// Execute a SQL statement that returns no rows.
    Execute {
        sql: String,
        params: Vec<JsonValue>,
        reply: oneshot::Sender<HcResult<usize>>,
    },
    /// Execute a SQL query and return rows as JSON maps.
    QueryJson {
        sql: String,
        params: Vec<JsonValue>,
        reply: oneshot::Sender<HcResult<Vec<Map<String, JsonValue>>>>,
    },
    /// Execute a SQL query and return Arrow `RecordBatch`es.
    QueryArrow {
        sql: String,
        reply: oneshot::Sender<HcResult<Vec<RecordBatch>>>,
    },
    /// Execute a sequence of SQL statements atomically (BEGIN / COMMIT).
    Transaction {
        statements: Vec<String>,
        reply: oneshot::Sender<HcResult<()>>,
    },
    /// Shut the manager thread down gracefully.
    Shutdown,
}

// ---------------------------------------------------------------------------
// DbManager – the public handle
// ---------------------------------------------------------------------------

/// A cheaply-cloneable handle to the DB manager thread.
#[derive(Clone)]
pub struct DbManager {
    tx: mpsc::Sender<DbCommand>,
}

impl DbManager {
    // -----------------------------------------------------------------------
    // Constructors
    // -----------------------------------------------------------------------

    /// Open (or create) a DuckDB database at the given file path.
    pub fn open<P: AsRef<Path>>(path: P) -> HcResult<Self> {
        let path = path.as_ref().to_path_buf();
        let conn = Connection::open(&path)?;
        Ok(Self::spawn(conn))
    }

    /// Open an in-memory DuckDB database.
    pub fn open_in_memory() -> HcResult<Self> {
        let conn = Connection::open_in_memory()?;
        Ok(Self::spawn(conn))
    }

    // -----------------------------------------------------------------------
    // Internal: spawn the manager thread
    // -----------------------------------------------------------------------

    fn spawn(conn: Connection) -> Self {
        let (tx, rx) = mpsc::channel::<DbCommand>(256);
        std::thread::spawn(move || run_manager(conn, rx));
        DbManager { tx }
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /// Execute a DDL / DML statement.  Returns the number of affected rows.
    pub async fn execute(&self, sql: impl Into<String>, params: Vec<JsonValue>) -> HcResult<usize> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(DbCommand::Execute {
                sql: sql.into(),
                params,
                reply: reply_tx,
            })
            .await
            .map_err(|_| HcError::Persistence("DB manager thread has stopped".into()))?;
        reply_rx
            .await
            .map_err(|_| HcError::Persistence("DB manager reply channel dropped".into()))?
    }

    /// Run a SELECT and return each row as a `serde_json::Map`.
    pub async fn query_json(
        &self,
        sql: impl Into<String>,
        params: Vec<JsonValue>,
    ) -> HcResult<Vec<Map<String, JsonValue>>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(DbCommand::QueryJson {
                sql: sql.into(),
                params,
                reply: reply_tx,
            })
            .await
            .map_err(|_| HcError::Persistence("DB manager thread has stopped".into()))?;
        reply_rx
            .await
            .map_err(|_| HcError::Persistence("DB manager reply channel dropped".into()))?
    }

    /// Run a SELECT and return Arrow `RecordBatch`es.
    pub async fn query_arrow(&self, sql: impl Into<String>) -> HcResult<Vec<RecordBatch>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(DbCommand::QueryArrow {
                sql: sql.into(),
                reply: reply_tx,
            })
            .await
            .map_err(|_| HcError::Persistence("DB manager thread has stopped".into()))?;
        reply_rx
            .await
            .map_err(|_| HcError::Persistence("DB manager reply channel dropped".into()))?
    }

    /// Execute several SQL statements atomically (BEGIN TRANSACTION / COMMIT).
    /// If any statement fails the transaction is rolled back.
    pub async fn transaction(&self, statements: Vec<String>) -> HcResult<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(DbCommand::Transaction {
                statements,
                reply: reply_tx,
            })
            .await
            .map_err(|_| HcError::Persistence("DB manager thread has stopped".into()))?;
        reply_rx
            .await
            .map_err(|_| HcError::Persistence("DB manager reply channel dropped".into()))?
    }

    /// Shut the manager thread down.
    pub async fn shutdown(&self) {
        let _ = self.tx.send(DbCommand::Shutdown).await;
    }
}

// ---------------------------------------------------------------------------
// Manager thread loop
// ---------------------------------------------------------------------------

fn run_manager(conn: Connection, mut rx: mpsc::Receiver<DbCommand>) {
    while let Some(cmd) = rx.blocking_recv() {
        match cmd {
            DbCommand::Shutdown => break,

            DbCommand::Execute { sql, params, reply } => {
                let result = execute_cmd(&conn, &sql, &params);
                let _ = reply.send(result);
            }

            DbCommand::QueryJson { sql, params, reply } => {
                let result = query_json_cmd(&conn, &sql, &params);
                let _ = reply.send(result);
            }

            DbCommand::QueryArrow { sql, reply } => {
                let result = query_arrow_cmd(&conn, &sql);
                let _ = reply.send(result);
            }

            DbCommand::Transaction { statements, reply } => {
                let result = transaction_cmd(&conn, &statements);
                let _ = reply.send(result);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Command implementations (run on the manager thread)
// ---------------------------------------------------------------------------

fn execute_cmd(conn: &Connection, sql: &str, params: &[JsonValue]) -> HcResult<usize> {
    let mut stmt = conn.prepare(sql)?;
    let bound: Vec<Box<dyn duckdb::ToSql>> = json_values_to_sql(params);
    let refs: Vec<&dyn duckdb::ToSql> = bound.iter().map(|b| b.as_ref()).collect();
    Ok(stmt.execute(params_from_iter(refs.iter().copied()))?)
}

fn query_json_cmd(
    conn: &Connection,
    sql: &str,
    params: &[JsonValue],
) -> HcResult<Vec<Map<String, JsonValue>>> {
    // Build the query, binding parameters.
    let bound: Vec<Box<dyn duckdb::ToSql>> = json_values_to_sql(params);
    let refs: Vec<&dyn duckdb::ToSql> = bound.iter().map(|b| b.as_ref()).collect();

    let mut stmt = conn.prepare(sql)?;
    let mut rows = stmt.query(params_from_iter(refs.iter().copied()))?;

    // Collect each row as a flat Vec<JsonValue>.  We probe column count by
    // repeatedly calling get_ref(i) until the index is out of range, which
    // sidesteps the borrow-checker conflict between &row and &rows.
    let mut raw_rows: Vec<Vec<JsonValue>> = Vec::new();

    while let Some(row) = rows.next()? {
        let mut vals = Vec::new();
        let mut i = 0usize;
        loop {
            match row.get_ref(i) {
                Ok(vr) => {
                    vals.push(value_ref_to_json(vr));
                    i += 1;
                }
                Err(duckdb::Error::InvalidColumnIndex(_)) => break,
                Err(e) => return Err(HcError::DuckDb(e)),
            }
        }
        raw_rows.push(vals);
    }

    if raw_rows.is_empty() {
        return Ok(vec![]);
    }

    // After iteration completes, the mutable borrow of `rows` is fully released
    // and the schema on `stmt` is now populated (at least one row was stepped).
    let col_names: Vec<String> = stmt.column_names();

    let result = raw_rows
        .into_iter()
        .map(|vals| {
            let mut map = Map::new();
            for (name, val) in col_names.iter().zip(vals) {
                map.insert(name.clone(), val);
            }
            map
        })
        .collect();

    Ok(result)
}

fn query_arrow_cmd(conn: &Connection, sql: &str) -> HcResult<Vec<RecordBatch>> {
    let mut stmt = conn.prepare(sql)?;
    let batches: Vec<RecordBatch> = stmt.query_arrow([])?.collect();
    Ok(batches)
}

fn transaction_cmd(conn: &Connection, statements: &[String]) -> HcResult<()> {
    conn.execute_batch("BEGIN TRANSACTION")?;
    for sql in statements {
        if let Err(e) = conn.execute_batch(sql) {
            let _ = conn.execute_batch("ROLLBACK");
            return Err(HcError::DuckDb(e));
        }
    }
    conn.execute_batch("COMMIT")?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Convert `serde_json::Value` parameters into boxed `duckdb::ToSql` impls.
fn json_values_to_sql(params: &[JsonValue]) -> Vec<Box<dyn duckdb::ToSql>> {
    params
        .iter()
        .map(|v| -> Box<dyn duckdb::ToSql> {
            match v {
                JsonValue::Null => Box::new(duckdb::types::Null),
                JsonValue::Bool(b) => Box::new(*b),
                JsonValue::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        Box::new(i)
                    } else if let Some(f) = n.as_f64() {
                        Box::new(f)
                    } else {
                        Box::new(n.to_string())
                    }
                }
                JsonValue::String(s) => Box::new(s.clone()),
                other => Box::new(other.to_string()),
            }
        })
        .collect()
}

/// Convert a `duckdb::types::ValueRef` into a `serde_json::Value`.
fn value_ref_to_json(vr: duckdb::types::ValueRef<'_>) -> JsonValue {
    use duckdb::types::ValueRef;
    match vr {
        ValueRef::Null => JsonValue::Null,
        ValueRef::Boolean(b) => JsonValue::Bool(b),
        ValueRef::TinyInt(i) => JsonValue::Number((i as i64).into()),
        ValueRef::SmallInt(i) => JsonValue::Number((i as i64).into()),
        ValueRef::Int(i) => JsonValue::Number((i as i64).into()),
        ValueRef::BigInt(i) => JsonValue::Number(i.into()),
        ValueRef::HugeInt(i) => JsonValue::String(i.to_string()),
        ValueRef::UTinyInt(u) => JsonValue::Number((u as u64).into()),
        ValueRef::USmallInt(u) => JsonValue::Number((u as u64).into()),
        ValueRef::UInt(u) => JsonValue::Number((u as u64).into()),
        ValueRef::UBigInt(u) => JsonValue::Number(u.into()),
        ValueRef::Float(f) => serde_json::Number::from_f64(f as f64)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        ValueRef::Double(f) => serde_json::Number::from_f64(f)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        ValueRef::Decimal(d) => {
            // Format as string then parse to f64 to avoid pulling in
            // rust_decimal's ToPrimitive trait directly.
            let s = d.to_string();
            s.parse::<f64>()
                .ok()
                .and_then(serde_json::Number::from_f64)
                .map(JsonValue::Number)
                .unwrap_or_else(|| JsonValue::String(s))
        }
        ValueRef::Text(bytes) => JsonValue::String(String::from_utf8_lossy(bytes).into_owned()),
        ValueRef::Blob(bytes) => JsonValue::String(String::from_utf8_lossy(bytes).into_owned()),
        ValueRef::Timestamp(_, i) => JsonValue::Number(i.into()),
        // All other complex types are serialised as their debug representation.
        other => JsonValue::String(format!("{other:?}")),
    }
}

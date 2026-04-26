// src/engine.rs
//
// Hot path engine loop: consumes raw messages, routes by table, buffers them
// in per-table TableBuffers, inserts to DuckDB, aggregates, delta-detects,
// and broadcasts.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use serde_json::Value;
use tokio::sync::{broadcast, watch};

use crate::aggregation::sql_gen::AggSqlGenerator;
use crate::aggregation::window;
use crate::config::{ColumnDef, CubeConfig, TableMode};
use crate::db_manager::{arrow::compute::concat_batches, DbManager};
use crate::delta::DeltaDetector;
use crate::error::HcResult;
use crate::ingest::IngestReceiver;
use crate::persistence;
use crate::publish::{batch_to_base64_arrow, DeltaEvent};
use crate::tables::TableBuffer;
use crate::transform::sql::value_to_sql;
use crate::transform::TransformPipeline;

/// Run the hot-path engine loop until `shutdown` fires or the ingest channel
/// closes.
///
/// - `raw_rx`      — typed `RawMessage` from the ingest source (Kafka, test harness, …)
/// - `broadcast_tx`— sends `DeltaEvent` to SSE subscribers after each window flush
/// - `shutdown`    — a `watch::Receiver<bool>`; breaks the loop when `true`
pub async fn run_hot_path(
    db: DbManager,
    config: CubeConfig,
    mut raw_rx: IngestReceiver,
    broadcast_tx: broadcast::Sender<DeltaEvent>,
    mut shutdown: watch::Receiver<bool>,
) -> HcResult<()> {
    // -------------------------------------------------------------------------
    // Setup
    // -------------------------------------------------------------------------
    let agg_gen = AggSqlGenerator::from_user_sql(&config.aggregation.publish.sql)?;
    let dimension_names: Vec<String> = agg_gen.dimensions().iter().map(|d| d.to_string()).collect();
    let mut detector = DeltaDetector::new(dimension_names, config.delta.epsilon);

    // Use the full aggregation SQL wrapped with a synthetic _key column.
    let agg_sql = agg_gen.full_aggregation_sql_with_key();

    // In the new config, transforms are per-source; no top-level transform.
    let _pipeline: Option<TransformPipeline> = None;

    // One buffer per table (reference tables have no buffer).
    let mut buffers: HashMap<String, TableBuffer> = config
        .tables
        .iter()
        .filter_map(|t| {
            let buf = match t.mode {
                TableMode::Append => TableBuffer::new_append(),
                TableMode::Replace => {
                    let key_cols = t.key_columns.clone().unwrap_or_default();
                    let schema_cols = t.schema.columns.iter().map(|c| c.name.clone()).collect();
                    TableBuffer::new_replace(key_cols, schema_cols)
                }
                TableMode::Reference => return None, // no ingest buffer
            };
            Some((t.name.clone(), buf))
        })
        .collect();

    let mut tick = tokio::time::interval(Duration::from_millis(config.window.interval_ms));
    let mut flush_counter = 0u64;

    // -------------------------------------------------------------------------
    // Main select loop
    // -------------------------------------------------------------------------
    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    tracing::info!(target: "engine", "Shutdown signal received");
                    break;
                }
            }

            _ = tick.tick() => {
                let start = Instant::now();
                let window_id = window::next_window_id();
                let mut any_rows = false;

                // Drain append buffers and insert rows.
                for (table_name, buf) in buffers.iter_mut() {
                    if let TableBuffer::Append(append_buf) = buf {
                        if append_buf.is_empty() { continue; }
                        let rows = append_buf.drain();
                        any_rows = true;
                        if let Some(table_cfg) = config.table(table_name) {
                            let insert_sql = build_insert_sql_for_table(
                                table_name,
                                &table_cfg.schema.columns,
                                &rows,
                                window_id,
                            );
                            db.execute(insert_sql, vec![]).await?;
                        }
                    }

                    // Replace buffers: truncate + re-insert surviving rows.
                    if let TableBuffer::Replace(replace_buf) = buf {
                        if replace_buf.is_empty() { continue; }
                        let rows = replace_buf.flush();
                        any_rows = true;
                        if let Some(table_cfg) = config.table(table_name) {
                            db.execute(format!("DELETE FROM {}", table_name), vec![]).await?;
                            for row in &rows {
                                let insert_sql = build_single_row_insert(
                                    table_name,
                                    &table_cfg.schema.columns,
                                    row,
                                );
                                db.execute(insert_sql, vec![]).await?;
                            }
                        }
                    }
                }

                if !any_rows { continue; }

                // Aggregate all retained slices.
                // DuckDB may return the result across multiple Arrow batches;
                // concatenate them before delta detection so no rows are missed.
                let batches = db.query_arrow(agg_sql.clone()).await?;

                if !batches.is_empty() {
                    let combined = if batches.len() == 1 {
                        batches[0].clone()
                    } else {
                        let schema = batches[0].schema();
                        concat_batches(&schema, &batches)
                            .unwrap_or_else(|_| batches[0].clone())
                    };
                    let (upserts, _deletes) = detector.detect(&combined);

                    if upserts.num_rows() > 0 {
                        if let Ok(b64) = batch_to_base64_arrow(&upserts) {
                            let _ = broadcast_tx.send(DeltaEvent {
                                base64_arrow: b64,
                                row_count: upserts.num_rows(),
                            });
                        }
                    }

                    tracing::debug!(
                        target: "aggregate",
                        "Window {}: {} upserts in {:?}",
                        window_id,
                        upserts.num_rows(),
                        start.elapsed()
                    );
                }

                // Persist window ID periodically
                flush_counter += 1;
                let interval = config.persistence.flush_interval;
                #[allow(clippy::manual_is_multiple_of)]
                if interval > 0 && flush_counter % interval == 0 {
                    let _ = persistence::save_window_id(&db, window_id).await;
                }
            }

            msg = raw_rx.recv() => {
                match msg {
                    Some(raw_msg) => {
                        if let Some(buf) = buffers.get_mut(&raw_msg.table) {
                            if let Some(table_cfg) = config.table(&raw_msg.table) {
                                match parse_row_for_table(&raw_msg.bytes, table_cfg) {
                                    Ok(row) => match buf {
                                        TableBuffer::Append(_) => buf.push_append(row),
                                        TableBuffer::Replace(_) => buf.upsert_replace(row),
                                    },
                                    Err(e) => tracing::warn!(
                                        target: "ingest",
                                        "Parse error for {}: {}",
                                        raw_msg.table,
                                        e
                                    ),
                                }
                            }
                        } else {
                            tracing::warn!(
                                target: "ingest",
                                "Unknown table: {}",
                                raw_msg.table
                            );
                        }
                    }
                    None => {
                        tracing::info!(target: "engine", "Ingest channel closed");
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_row_for_table(bytes: &[u8], table_cfg: &crate::config::TableConfig) -> HcResult<Vec<Value>> {
    let v: Value = serde_json::from_slice(bytes)
        .map_err(|e| crate::error::HcError::Ingest(e.to_string()))?;
    Ok(table_cfg
        .schema
        .columns
        .iter()
        .map(|c| v.get(&c.name).cloned().unwrap_or(Value::Null))
        .collect())
}

/// Build a bulk `INSERT INTO <table>` statement for all rows in a window.
///
/// Column list: schema columns in order, plus `_window_id`.
fn build_insert_sql_for_table(
    table_name: &str,
    columns: &[ColumnDef],
    rows: &[Vec<Value>],
    window_id: u64,
) -> String {
    let col_names: Vec<String> = columns
        .iter()
        .map(|c| c.name.clone())
        .chain(std::iter::once("_window_id".to_string()))
        .collect();
    let col_list = col_names.join(", ");

    let value_tuples: Vec<String> = rows
        .iter()
        .map(|row| {
            let mut literals: Vec<String> = row
                .iter()
                .zip(columns.iter())
                .map(|(v, col)| value_to_sql(v, &col.col_type))
                .collect();
            literals.push(window_id.to_string());
            format!("({})", literals.join(", "))
        })
        .collect();

    format!(
        "INSERT INTO {} ({}) VALUES {}",
        table_name,
        col_list,
        value_tuples.join(", ")
    )
}

/// Build a single-row `INSERT INTO <table>` for replace-mode rows (no _window_id).
fn build_single_row_insert(
    table_name: &str,
    columns: &[ColumnDef],
    row: &[Value],
) -> String {
    let col_list = columns
        .iter()
        .map(|c| c.name.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let vals = row
        .iter()
        .zip(columns.iter())
        .map(|(v, col)| value_to_sql(v, &col.col_type))
        .collect::<Vec<_>>()
        .join(", ");
    format!("INSERT INTO {} ({}) VALUES ({})", table_name, col_list, vals)
}

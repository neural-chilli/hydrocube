// src/engine.rs
//
// Hot path engine loop: consumes raw messages, buffers them in-window,
// inserts to DuckDB slices, aggregates, delta-detects, and broadcasts.

use std::time::{Duration, Instant};

use serde_json::Value;
use tokio::sync::{broadcast, mpsc, watch};

use crate::aggregation::sql_gen::AggSqlGenerator;
use crate::aggregation::window;
use crate::config::{ColumnDef, CubeConfig};
use crate::db_manager::{arrow::compute::concat_batches, DbManager};
use crate::delta::DeltaDetector;
use crate::error::HcResult;
use crate::ingest::parser::JsonParser;
use crate::persistence;
use crate::publish::{batch_to_base64_arrow, DeltaEvent};
use crate::transform::sql::value_to_sql;
use crate::transform::TransformPipeline;

/// Run the hot-path engine loop until `shutdown` fires or the ingest channel
/// closes.
///
/// - `raw_rx`      — byte messages from the ingest source (Kafka, test harness, …)
/// - `broadcast_tx`— sends `DeltaEvent` to SSE subscribers after each window flush
/// - `shutdown`    — a `watch::Receiver<bool>`; breaks the loop when `true`
pub async fn run_hot_path(
    db: DbManager,
    config: CubeConfig,
    mut raw_rx: mpsc::Receiver<Vec<u8>>,
    broadcast_tx: broadcast::Sender<DeltaEvent>,
    mut shutdown: watch::Receiver<bool>,
) -> HcResult<()> {
    // -------------------------------------------------------------------------
    // Setup
    // -------------------------------------------------------------------------
    // Use the first table's columns as the schema for the hot path.
    // In future tasks this will be per-table; for now use an empty slice if no tables.
    let hot_columns: Vec<crate::config::ColumnDef> = config.tables.first()
        .map(|t| t.schema.columns.clone())
        .unwrap_or_default();
    let parser = JsonParser::new(&hot_columns);

    let agg_gen = AggSqlGenerator::from_user_sql(&config.aggregation.publish.sql)?;
    let dimension_names: Vec<String> = agg_gen.dimensions().iter().map(|d| d.to_string()).collect();
    let mut detector = DeltaDetector::new(dimension_names);

    // Use the full aggregation SQL wrapped with a synthetic _key column.
    // _key is a pipe-delimited concatenation of all GROUP BY dimensions,
    // used by the Perspective viewer as a stable row index for true upserts.
    let agg_sql = agg_gen.full_aggregation_sql_with_key();

    // In the new config, transforms are per-source; no top-level transform.
    let pipeline: Option<TransformPipeline> = None;

    let mut buffer: Vec<Vec<Value>> = Vec::new();
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
                if buffer.is_empty() {
                    continue;
                }

                let start = Instant::now();
                let window_id = window::next_window_id();
                let row_count = buffer.len();

                // Transform if configured
                let rows = if let Some(ref pipeline) = pipeline {
                    pipeline
                        .execute(&db, &hot_columns, std::mem::take(&mut buffer))
                        .await?
                } else {
                    std::mem::take(&mut buffer)
                };

                if rows.is_empty() {
                    continue;
                }

                // Insert rows into slices table
                let insert_sql = build_insert_sql(&hot_columns, &rows, window_id);
                db.execute(insert_sql, vec![]).await?;

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
                        "Window {}: {} rows -> {} upserts in {:?}",
                        window_id,
                        row_count,
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
                    Some(bytes) => {
                        match parser.parse(&bytes) {
                            Ok(row) => buffer.push(row),
                            Err(e) => tracing::warn!(target: "ingest", "Parse error: {}", e),
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

/// Build a bulk `INSERT INTO slices` statement for all rows in a window.
///
/// Column list: schema columns in order, plus `_window_id`.
/// Values: one tuple per row, with SQL-safe literals from `value_to_sql`.
fn build_insert_sql(columns: &[ColumnDef], rows: &[Vec<Value>], window_id: u64) -> String {
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
            // Append the window_id as an integer literal.
            literals.push(window_id.to_string());
            format!("({})", literals.join(", "))
        })
        .collect();

    format!(
        "INSERT INTO slices ({}) VALUES {}",
        col_list,
        value_tuples.join(", ")
    )
}

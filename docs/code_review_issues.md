# HydroCube Code Review

**Date:** 14 April 2026
**Scope:** Full codebase review — all source, tests, config, CI, and design docs.
**Status:** Pre-release. Core engine implemented, not yet wired end-to-end.

---

## Critical — Will Bite You at Runtime

### 1. SSE `SseStream` is a busy-loop CPU spin

`src/web/sse.rs` — the `Stream` implementation calls `cx.waker().wake_by_ref()` on `TryRecvError::Empty`, which immediately re-schedules the task. This pegs one CPU core at 100% per connected SSE client.

`tokio::sync::broadcast::Receiver` doesn't natively integrate with `poll`. The `try_recv` / wake pattern is fundamentally wrong here.

**Fix:** Use the async `.recv()` method via `async_stream` or `tokio_stream::wrappers::BroadcastStream`:

```rust
pub fn sse_stream(
    rx: broadcast::Receiver<DeltaEvent>,
) -> impl Stream<Item = Result<Event, Infallible>> {
    async_stream::stream! {
        let mut rx = rx;
        loop {
            match rx.recv().await {
                Ok(event) => {
                    yield Ok(Event::default()
                        .event("delta")
                        .data(event.base64_arrow));
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(target: "sse", "client lagged by {} events", n);
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    }
}
```

---

### 2. `snapshot_handler` and `query_handler` silently drop all but the last Arrow batch

`src/web/api.rs` — `query_arrow` returns `Vec<RecordBatch>`, but the handler loops and overwrites:

```rust
for batch in &batches {
    row_count += batch.num_rows();
    match batch_to_base64_arrow(batch) {
        Ok(b64) => all_b64 = b64,  // ← overwrites previous batches
    }
}
```

If DuckDB returns multiple batches (which it does for larger result sets), only the last one is serialised. All prior rows are counted but never sent.

**Fix:** Concatenate batches before serialising:

```rust
if batches.is_empty() {
    return Ok(Json(SnapshotResponse { row_count: 0, arrow_ipc_b64: String::new() }));
}

let schema = batches[0].schema();
let merged = duckdb::arrow::compute::concat_batches(&schema, &batches)
    .map_err(|e| ApiError(HcError::Publish(format!("concat error: {e}"))))?;

let b64 = batch_to_base64_arrow(&merged)?;
```

Same fix needed in `query_handler`.

---

### 3. Lua transform result loses column ordering

`src/transform/lua.rs` — `lua_result_to_rows` iterates `row_tbl.pairs::<LuaValue, LuaValue>()` and collects values into a `Vec`. Lua table pair iteration order for string keys is **hash-table order**, not insertion order. The resulting `Vec<Value>` rows have columns in an unpredictable order that will silently misalign with the schema.

**Fix:** Iterate by known column names instead of pairs:

```rust
fn lua_result_to_rows(result: LuaValue, col_names: &[String]) -> HcResult<Vec<Vec<Value>>> {
    // ...
    for pair in tbl.sequence_values::<mlua::Value>() {
        match row_val {
            LuaValue::Table(row_tbl) => {
                let row: Vec<Value> = col_names.iter().map(|name| {
                    match row_tbl.get::<&str, LuaValue>(name.as_str()) {
                        Ok(v) => lua_to_json(v),
                        Err(_) => Value::Null,
                    }
                }).collect();
                rows.push(row);
            }
            // ...
        }
    }
}
```

Note: the function signature currently discards `_col_names` — it needs to use them.

---

### 4. SQL transform result has the same column ordering problem

`src/transform/sql.rs` — converts `serde_json::Map` rows to `Vec<Value>` via `map.into_values().collect()`. Without the `preserve_order` feature on `serde_json`, `Map` is backed by `BTreeMap`, so `into_values()` yields **alphabetical** key order, not query column order.

A query returning `(id, value)` would produce `[id, value]` by coincidence (alphabetical), but `(book, avg_price)` would produce `[avg_price, book]` — silently swapped.

**Fix:** Either enable `serde_json = { version = "1", features = ["preserve_order"] }` in `Cargo.toml`, or collect values by iterating column names explicitly from the query result schema.

---

## Significant — Design and Correctness Concerns

### 5. `arrow` crate version mismatch risk

`Cargo.toml` declares `arrow = "58"` as a direct dependency. `publish.rs` imports `arrow::ipc::writer::StreamWriter` from this crate, but receives `RecordBatch` values that are actually `duckdb::arrow::record_batch::RecordBatch` (re-exported by the `duckdb` crate).

These are the same concrete type only as long as `duckdb` internally depends on `arrow = "58"`. If either crate bumps its Arrow version independently, you get confusing type mismatch errors at compile time.

**Fix:** Remove the direct `arrow` dependency and use `duckdb::arrow` exclusively throughout. Re-export what you need from `db_manager.rs`:

```rust
// db_manager.rs
pub use duckdb::arrow::ipc::writer::StreamWriter;
pub use duckdb::arrow::record_batch::RecordBatch;
```

---

### 6. Global atomics for window state are a testing hazard

`src/aggregation/window.rs` — `WINDOW_ID` and `COMPACTION_CUTOFF` are `static AtomicU64` values shared across the entire process. Tests that call `next_window_id()`, `restore_state()`, or `set_compaction_cutoff()` mutate global state and can interfere with each other when run in parallel.

The `window.rs` unit tests manually reset to known baselines, but integration tests don't — flaky failures are likely as the test suite grows.

**Options:**
- Wrap window state in a struct passed through `AppState` / function params (cleanest, most testable).
- At minimum, run `cargo test -- --test-threads=1` in CI and document the constraint.

---

### 7. `Timestamp` values serialised as raw integers in JSON

`src/db_manager.rs` — `value_ref_to_json` handles timestamps by returning the raw microsecond offset:

```rust
ValueRef::Timestamp(_, i) => JsonValue::Number(i.into()),
```

The aggregation SQL uses `MAX(trade_time)` which produces a `TIMESTAMP`. JSON consumers (the UI, ad-hoc query results) receive an opaque integer like `1704067200000000` with no indication of unit or epoch. This is unusable without out-of-band knowledge.

**Fix:** Format as ISO 8601:

```rust
ValueRef::Timestamp(unit, i) => {
    use duckdb::types::TimeUnit;
    let micros = match unit {
        TimeUnit::Second => i * 1_000_000,
        TimeUnit::Millisecond => i * 1_000,
        TimeUnit::Microsecond => i,
        TimeUnit::Nanosecond => i / 1_000,
    };
    let secs = micros / 1_000_000;
    let nanos = ((micros % 1_000_000) * 1_000) as u32;
    match chrono::DateTime::from_timestamp(secs, nanos) {
        Some(dt) => JsonValue::String(dt.to_rfc3339()),
        None => JsonValue::Number(i.into()),
    }
}
```

---

### 8. `DbManager` thread leaks if not explicitly shut down

`DbManager::spawn` creates an OS thread that blocks on `rx.blocking_recv()` indefinitely. If the `DbManager` is dropped without calling `shutdown()`, the thread leaks — it will block forever waiting for a message that never arrives.

The `tokio::sync::mpsc::Sender` being dropped will cause `blocking_recv()` to return `None` and the thread will exit, so this is actually safe in practice. However, `DbManager` is `Clone`, so the channel only closes when *all* clones are dropped. In the main function, clones are handed to compaction and web server tasks — if main exits before those tasks complete, the thread may linger.

**Fix:** Either add a `Drop` impl that sends `Shutdown`, or document that `shutdown()` must be called before exit (which you do in `main.rs` — just make sure all error paths also call it).

---

## Minor — Consistency and Cleanup

### 9. Inconsistent SQL parameterisation style

Some callers use `?` placeholders with `Vec<JsonValue>` params (e.g. `persistence.rs` — `INSERT INTO _cube_metadata ... VALUES (?, 0, 0, NOW())`), while others inline values via `format!()` (e.g. `compaction.rs` — `format!("DELETE FROM slices WHERE _window_id <= {}", cutoff)`).

Both work correctly (the inlined values are always `u64` so there's no injection risk), but the inconsistency makes it harder to reason about safety at a glance. Pick one style — parameterised is generally preferable.

---

### 10. Basic auth middleware not wired into the router

`src/auth/basic.rs` defines the middleware, but `src/web/server.rs` never applies it. The server starts without auth regardless of `config.auth`. Not a bug yet (auth is documented as incomplete), but easy to forget.

---

### 11. `futures` crate compiled unconditionally but only used behind `#[cfg(feature = "kafka")]`

`Cargo.toml` lists `futures = "0.3"` as an always-compiled dependency, but it's only imported inside `#[cfg(feature = "kafka")]` blocks in `kafka.rs`. Moving it behind the feature gate saves compile time on `--no-default-features` builds.

---

### 12. Plan doc references `Persistence::method()` but code uses free functions

The implementation plan (Task 5, etc.) shows `Persistence::init()`, `Persistence::verify_config_hash()` as associated functions on a struct. The actual code in `persistence.rs` uses module-level free functions (`persistence::init()`, `persistence::verify_config_hash()`). The code is correct and arguably cleaner — the plan is just stale.

---

### 13. `compaction.rs` consumes `self` in `run` but only borrows in `compact_cycle`

`CompactionThread::run` takes `self` (owned), which means the compaction thread owns the `CompactionThread`. This is fine, but `compact_cycle` takes `&self`. If you ever need to access the compaction thread after spawning (e.g. for a manual compaction trigger via the API), you'll need to restructure. Low priority but worth noting.

---

### 14. CI doesn't run `cargo test` for the full default feature set

`.github/workflows/ci.yml` runs all checks with `--no-default-features`, which is correct (avoids the librdkafka build dependency), but it means the Kafka code path is never compiled in CI. The comment explains the rationale — just be aware that Kafka compilation regressions won't be caught until someone builds locally with defaults.

---

## What's Good

- **DB manager channel pattern** is the right design for DuckDB's single-connection constraint. Clean separation, no mutex contention, atomic swap via `Transaction` variant.
- **Delta detector** is simple, correct, and efficient — hash comparison keeps memory constant per group.
- **Config validation** catches real issues early (window bounds, missing Kafka fields, retention duration parsing).
- **Test coverage** is thorough. Tests properly construct configs in code rather than relying on file I/O, and avoid Kafka dependencies.
- **Persistence lifecycle** (init → verify hash → restore state → shutdown save) is well thought through. The `--rebuild` from Parquet path is sound.
- **Compaction design** (export → delete → advance cutoff → prune) correctly separates concerns and the safety buffer prevents hot-path/compaction row overlap.
- **Separation between spec and plan** is disciplined — the spec describes *what*, the plan describes *how*, and the code follows both.
- **Error types** are well-structured with meaningful variants and clean exit codes.

---

## Recommended Fix Priority

| Priority | Issue | Effort |
|----------|-------|--------|
| **P0** | #1 SSE busy-loop spin | 30 min |
| **P0** | #2 Arrow batch concatenation bug | 20 min |
| **P0** | #3 Lua column ordering | 30 min |
| **P0** | #4 SQL transform column ordering | 10 min |
| **P1** | #5 Arrow crate version coupling | 20 min |
| **P1** | #7 Timestamp serialisation | 20 min |
| **P2** | #6 Global atomics / test isolation | 1–2 hr |
| **P2** | #8 DbManager shutdown safety | 15 min |
| **P3** | #9–14 Consistency and cleanup | Ad hoc |
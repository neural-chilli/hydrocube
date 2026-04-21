# Ingest Layer Design

**Date:** 2026-04-21
**Status:** Approved for implementation planning

---

## Problem

HydroCube currently supports a single ingest source feeding a single append-only table (`slices`). Real deployments need multiple simultaneous sources — different protocols, different message formats — feeding tables with different storage semantics (append, last-value-cache, static reference data). The engine and config model must be extended to support this without breaking existing deployments.

---

## Decisions

### Option B: Explicit tables + sources

Tables and sources are declared as separate lists. Tables own their schema and storage mode; sources reference tables by name. Multiple sources may feed the same table provided they write to non-overlapping key ranges (for replace tables) or any rows (for append tables).

This separation was chosen over a flat sources list (where tables are implicitly defined by the first source that references them) because it makes schema and mode declarations explicit and unambiguous, and enforces a single authoritative contract per table regardless of how many sources feed it.

### Backward compatibility

At parse time, if the old top-level `source:` and `schema:` keys are detected, they are silently mapped to the new model:
- `schema:` → `tables[0]` with `name: slices, mode: append`
- `source:` → `sources[0]` with `table: slices`

Existing `cube.yaml` files require zero changes.

---

## Config model

```yaml
tables:
  - name: slices
    mode: append
    schema:
      columns:
        - name: trade_id
          type: VARCHAR
        - name: book
          type: VARCHAR
        - name: notional
          type: DOUBLE

  - name: market_data
    mode: replace
    key_columns: [curve, tenor]
    schema:
      columns:
        - name: curve
          type: VARCHAR
        - name: tenor
          type: VARCHAR
        - name: rate
          type: DOUBLE

  - name: instrument_master
    mode: reference
    schema:
      columns:
        - name: instrument_id
          type: VARCHAR
        - name: sector
          type: VARCHAR

sources:
  - type: kafka
    brokers: ["localhost:9092"]
    topic: trades.executed
    group_id: hydrocube-trades
    table: slices
    format: json

  - type: nats
    url: nats://localhost:4222
    subject: rates.live
    table: market_data
    format: json

  - type: http
    table: slices
    format: json

  - type: file
    path: /data/instruments.csv
    table: instrument_master
    format: csv
    refresh: startup
```

---

## Table modes

### `append`

The existing `slices` behaviour, unchanged. Each message inserts a new row with an auto-assigned `_window_id`. Rows accumulate over time and are compacted on schedule. Use for event data where every record matters (trades, sensor events, log entries).

Transforms run **on arrival** — every message is transformed before entering the insert buffer.

### `replace` (Last Value Cache)

Each message upserts based on `key_columns`. The DuckDB table carries a `PRIMARY KEY` constraint on those columns. Messages do not go to DuckDB directly on arrival; instead they update an in-memory `HashMap<KeyString, RawMessage>` per table (the conflation map). On each window flush, the map is bulk-upserted into DuckDB via `INSERT OR REPLACE`. The table stays constant size regardless of tick rate.

Transforms run **on flush** — only the surviving value per key is transformed, not every arriving tick. This is a deliberate optimisation: at 100,000 ticks/second across 10,000 instruments with a 1-second window, this reduces Lua calls from 100,000 to 10,000 per window.

Two sources feeding the same replace table must own non-overlapping key ranges. Last-writer-wins within a window if they don't — this is a configuration error, not something the engine resolves.

### `reference`

Read-only lookup data loaded from a file source (not a streaming source). Loaded into DuckDB at startup or on a refresh schedule. Available to aggregation SQL as a join target. Not subject to compaction or retention — managed independently of the window cycle.

Three refresh strategies:

| Strategy | Behaviour |
|---|---|
| `startup` | Loaded once when the process starts |
| `interval: 1h` | Reloaded on a fixed schedule; old rows replaced atomically |
| `watch` | File-system watch; reloaded when the file changes |

---

## Source protocols

All source types implement the existing `IngestSource` trait (`run(tx, shutdown)`). The engine loop is unchanged — sources produce `RawMessage` values on a shared channel; the engine routes them to the appropriate per-table buffer.

### Kafka

Existing implementation, unchanged. Brokers, topic, group_id, format. Offsets committed after each window flush (at-least-once delivery).

### NATS

Two sub-modes reflecting NATS's own delivery guarantees:

```yaml
# Core NATS — at-most-once. Appropriate for replace/LVC tables
# where a missed tick is corrected by the next one.
- type: nats
  url: nats://localhost:4222
  subject: rates.live
  table: market_data
  format: json
  mode: core

# JetStream — at-least-once, ack after window flush.
# Appropriate for append tables where every message must land.
- type: nats
  url: nats://localhost:4222
  stream: TRADES
  consumer: hydrocube-positions
  table: slices
  format: json
  mode: jetstream
```

### HTTP POST

Exposes a per-table ingest endpoint on the existing web server:

```
POST /ingest/{table_name}
Content-Type: application/json    # single JSON object or JSON array
Content-Type: text/csv            # CSV with header row
```

Returns `200 {"accepted": N}` where N is the number of rows queued. Auth is enforced identically to all other endpoints. Rate limited to 10,000 requests/minute by default (configurable); returns `429` when exceeded. Useful for webhooks, testing, serverless senders, and any source that can make an HTTP call.

### File

Batch load from a local file path. Supported formats: CSV (headers map to column names), Parquet (schema inferred from file), JSON Lines (one object per line). Refresh strategy is declared on the source. Primarily used for reference tables; can also seed an append table at startup.

```yaml
- type: file
  path: /data/closing_positions.parquet
  table: slices
  format: parquet
  refresh: startup
```

---

## Schema format support

Column mapping is by name for all formats. Extra fields in the message are ignored. Missing fields become NULL. Type coercion follows the table's declared schema.

### JSON
Existing behaviour, unchanged.

### CSV
First row treated as column headers. All values coerced from strings to declared column types.

### Parquet
Schema inferred from the file. Column names matched to table schema by name.

### Avro (inline schema)

Schema defined as a JSON string in the source config. No external schema registry required.

```yaml
- type: kafka
  topic: trades.avro
  table: slices
  format: avro
  avro_schema: |
    {
      "type": "record",
      "name": "Trade",
      "fields": [
        {"name": "trade_id", "type": "string"},
        {"name": "notional", "type": "double"},
        {"name": "book",     "type": "string"}
      ]
    }
```

### Protobuf (inline schema)

`.proto` content inline, plus the message type name to deserialise:

```yaml
- type: kafka
  topic: rates.proto
  table: market_data
  format: protobuf
  proto_schema: |
    syntax = "proto3";
    message RateUpdate {
      string curve  = 1;
      string tenor  = 2;
      double rate   = 3;
    }
  proto_message: RateUpdate
```

Both Avro and Protobuf schemas are compiled once at startup. A mismatch between the inline schema and arriving messages increments `parse_errors` on `/api/status` and the message is skipped — the engine does not crash.

---

## Engine architecture

### Tagged message type

```rust
struct RawMessage {
    table:  String,   // target table name
    bytes:  Vec<u8>,  // raw wire bytes
    format: Format,   // Json | Avro | Protobuf | Csv | Parquet
}
```

Each source gets a clone of the channel sender. All sources share one channel; the engine routes `RawMessage` values to the appropriate per-table buffer on receipt.

### Per-table buffers

- **Append table:** `Vec<RawMessage>` — drained and batch-inserted at window flush
- **Replace table:** `HashMap<KeyString, RawMessage>` — surviving entries transformed and bulk-upserted at window flush
- **Reference table:** not part of the message flow; managed by a separate loader task

### Window flush sequence

```
1. For each append table:
       transform messages → batch INSERT INTO <table>
2. For each replace table:
       transform surviving HashMap entries → INSERT OR REPLACE INTO <table>
3. Run aggregation SQL (references any declared table freely)
4. Delta detect on aggregation result → broadcast via SSE
5. Commit Kafka offsets / ack NATS JetStream messages
```

### Schema validation at startup

Before accepting any messages the engine validates every source against its target table:
- Avro/Protobuf field names checked for compatibility with table column names
- `key_columns` on replace tables verified to exist in the table schema
- Any mismatch exits with a structured error before the process accepts connections

### Runtime error handling

Malformed or incompatible messages never crash the engine. Two counters are tracked per table and exposed on `GET /api/status`:

| Counter | Meaning |
|---|---|
| `parse_errors` | Message could not be deserialised |
| `schema_errors` | Deserialised but column types incompatible with table schema |

Failing messages are logged at `warn` level with a truncated payload excerpt, then dropped. The counter is reset on process restart.

---

## What this deliberately does not include

- External schema registry integration (Confluent, AWS Glue) — inline schema covers v1
- S3/GCS URI support for file sources — local paths only for now
- Schema evolution / ALTER TABLE — still requires `--rebuild`
- Per-source backpressure signalling — covered in the Output Layer design
- Streaming joins across sources at the ingest layer — joins happen in aggregation SQL, not during ingest

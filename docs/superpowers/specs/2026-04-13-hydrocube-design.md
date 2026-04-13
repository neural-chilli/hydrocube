# HydroCube — Design Specification

Real-time aggregation engine. Ingest from message queues, aggregate with DuckDB, visualise with Perspective. One binary.

## What Is This?

HydroCube replaces enterprise OLAP tools like ActivePivot/Atoti for teams that need real-time aggregated views of streaming data — trading positions, order flow, risk metrics, operational dashboards — without the cost, complexity, or operational overhead.

**One binary. YAML config. SQL aggregations. No MDX. No JVM. No cluster.**

---

## Positioning

ActivePivot charges six to seven figures annually, requires a JVM cluster, proprietary MDX queries, dedicated consultants, and months of implementation. HydroCube delivers 80% of the capability at a fraction of the cost and complexity.

| | ActivePivot | HydroCube |
|---|---|---|
| **Runtime** | JVM cluster | Single binary |
| **Configuration** | Java/Spring + MDX | YAML + SQL |
| **Startup** | Minutes (cube rebuild from source) | Seconds (DuckDB persistence) |
| **Crash recovery** | Full replay from source | Sub-second (persistent state) |
| **GC pauses** | Yes (Java) | None (Rust/C) |
| **Query language** | MDX | SQL |
| **Visualisation** | Proprietary UI | Perspective (open source, WASM) |
| **Scaling** | Cluster coordination | More containers |
| **Deployment** | Weeks | Minutes |
| **Price** | £500K+/year | See Licensing |

---

## Architecture

```
                                                    +------------------+
                                                +-->| Browser (A)      |
                                                |   | Perspective      |
                                                |   | SSE subscription |
                                                |   +------------------+
+----------+    +----------------------------+  |   +------------------+
|  Kafka   |--->|        HydroCube           |  +-->| Browser (B)      |
|          |    |                            |  |   | Perspective      |
|          |    |  +--------+  +----------+  |  |   | SSE subscription |
|          |--->|  | Ingest |->| 1s Window|  |  |   +------------------+
|          |    |  +--------+  +----+-----+  |  |   +------------------+
+----------+    |              +----v-----+  |  +-->| Browser (C)      |
                |              |  DuckDB  |  |      | Perspective      |
                |              |  Slices  |  |      | SSE subscription |
                |              +----+-----+  |      +------------------+
                |              +----v-----+  |
                |  +--------+  | Aggregate|  |
                |  |Compactn|  | consol + |--+--> NATS pub (Arrow IPC)
                |  | thread |  | recent   |  |
                |  |(bg, no |  +----+-----+  |
                |  | pause) |  +----v-----+  |
                |  +---+----+  |  Delta   |  |
                |      |       |  Detect  |  |
                |      v       +----+-----+  |
                |  Parquet          |        |
                |  partition   +----v-----+  |
                |  + atomic    |   SSE    |  |
                |  cutoff      | Broadcast|  |
                |  swap        +----------+  |
                |                            |
                |  +----------------------+  |
                |  |   Embedded Web UI    |  |
                |  |  Perspective + SSE   |  |
                |  |  Bootstrap + Query   |  |
                |  +----------------------+  |
                |                            |
                |  +----------------------+  |
                |  |   DB Manager Thread  |  |
                |  |  Single DuckDB conn  |  |
                |  |  Channel-based SQL   |  |
                |  +----------------------+  |
                +----------------------------+
```

### Data Flow

1. **Ingest** drains the source (Kafka) as fast as possible into a memory buffer
2. **Window** closes every N milliseconds (configurable, default 1 second), flushing the buffer to the `slices` table with a `_window_id`
3. **Aggregate** queries `consolidated UNION ALL recent slices` with the user's GROUP BY — any SQL aggregate works (SUM, AVG, COUNT DISTINCT, percentiles — no special merge logic)
4. **Delta detect** compares the new aggregate result against the previous window, identifies changed groups
5. **Publish** sends only the changed group rows to NATS as Arrow IPC payloads
6. **SSE broadcast** pushes deltas to connected browser clients via Server-Sent Events
7. **Compact** (background thread) periodically rebuilds `consolidated` from itself plus older slices, writes old slices to time-partitioned Parquet files, then atomically swaps via the DB manager. The hot path never pauses — it reads only slices above the cutoff
8. **Browser** receives deltas via SSE, Perspective merges them client-side

---

## DB Manager Thread

All DuckDB access flows through a single manager thread. Other threads (hot path, compaction, web API) send SQL commands via a `tokio::mpsc` channel and receive results via a `oneshot` sender.

```rust
enum DbCommand {
    /// Execute a single SQL statement, return result
    Query {
        sql: String,
        params: Vec<Value>,
        reply: oneshot::Sender<Result<QueryResult>>,
    },
    /// Execute multiple statements as a single transaction (no interleaving)
    Transaction {
        statements: Vec<String>,
        reply: oneshot::Sender<Result<()>>,
    },
}
```

The manager thread holds the single `duckdb::Connection` and drains the channel sequentially. The `Transaction` variant ensures the compaction table swap executes atomically — no other SQL can interleave between the DROP and RENAME.

This design means:
- No mutex on the DuckDB connection
- No `Send` requirement on `duckdb::Connection`
- Compaction's heavy work (CREATE TABLE AS SELECT) blocks the hot path for its duration, but compaction breaks its cycle into discrete steps (see Compaction section) so the hot path can interleave between them
- The web API's ad-hoc queries also flow through the manager — they queue behind hot path work, which is fine (ad-hoc queries are interactive, not latency-critical)

---

## Core Engine

**DuckDB** (embedded, via Rust `duckdb` crate) — provides SQL aggregation, columnar storage, Arrow IPC export, and persistence to disk. Single embedded connection managed by the DB manager thread.

**NATS** (optional) — message transport for delta publication to external subscribers. Used for system-to-system integration when configured. Not used for browser delivery (see SSE). If `publish.nats` is omitted from config, NATS is not started and `async-nats` is unused at runtime.

**Perspective** (FINOS) — client-side WASM-based pivot table and charting. Consumes Arrow IPC directly with zero serialisation overhead. Handles final slice-and-dice, pivoting, filtering, and visualisation.

---

## Configuration

A single YAML file defines one cube.

```yaml
# cube.yaml
name: trading_positions
description: Real-time position aggregation by book

# --- Source ---
source:
  type: kafka                            # kafka (v1.0) | nats | nats_jetstream | amqp | redis_streams | file_watch (v1.1)
  brokers: ["localhost:9092"]
  topic: "trades.executed"
  group_id: "hydrocube-positions"
  format: json                           # json (v1.0) | csv | ndjson | avro (v1.1)

  # --- NATS (v1.1) ---
  # type: nats
  # url: "nats://localhost:4222"
  # subject: "trades.executed"
  # format: json

  # --- NATS JetStream (v1.1) ---
  # type: nats_jetstream
  # url: "nats://localhost:4222"
  # stream: "TRADES"
  # consumer: "hydrocube-positions"
  # subject: "trades.executed"
  # format: json

  # --- AMQP 1.0 (v1.1) ---
  # type: amqp
  # url: "amqp://mq.internal:5672"
  # queue: "TRADE.EVENTS"
  # format: json

  # --- Redis Streams (v1.1) ---
  # type: redis_streams
  # url: "redis://localhost:6379"
  # stream: "trades"
  # group: "hydrocube-positions"
  # consumer: "cube-1"
  # format: json

  # --- File Watch (v1.1) ---
  # type: file_watch
  # path: "/data/incoming/"
  # pattern: "*.csv"
  # format: csv
  # archive_dir: "/data/processed/"

# --- Schema ---
# Column definitions for the slices table (the flat rows after transform).
# Types are DuckDB types.
schema:
  columns:
    - name: trade_id
      type: VARCHAR
    - name: book
      type: VARCHAR
    - name: desk
      type: VARCHAR
    - name: instrument
      type: VARCHAR
    - name: instrument_type
      type: VARCHAR
    - name: currency
      type: VARCHAR
    - name: quantity
      type: DOUBLE
    - name: price
      type: DOUBLE
    - name: notional
      type: DOUBLE
    - name: side
      type: VARCHAR
    - name: trade_time
      type: TIMESTAMP

# --- Transform ---
# Optional pipeline to reshape raw messages before they enter slices.
# Each step is either SQL or Lua. Steps execute in order.
# If omitted, raw messages are inserted directly using schema column mapping.
#
# transform:
#   # Simple SQL unnest (e.g. explode a tenor ladder)
#   - type: sql
#     sql: |
#       SELECT trade_id, book, desk, ccy, timestamp,
#              unnest.tenor, unnest.delta, unnest.gamma
#       FROM raw_buffer, UNNEST(tenors) AS unnest
#
#   # Lua script for complex parsing/enrichment
#   - type: lua
#     script: transforms/decode_fpml.lua
#     function: transform_batch   # receives array of messages, returns array of row tables
#     init: init                  # optional startup function for loading reference data
#
#   # Composable: Lua parses, SQL reshapes
#   - type: lua
#     script: transforms/parse.lua
#     function: transform_batch
#   - type: sql
#     sql: "SELECT * FROM parsed WHERE notional > 0"

# --- Aggregation ---
# Pure SQL. Dimensions are inferred from GROUP BY columns.
# Measures are inferred from aggregate expressions in SELECT.
# Any DuckDB aggregate function works — SUM, AVG, COUNT DISTINCT,
# MEDIAN, percentiles, window functions. No MDX. No config DSL.
aggregation:
  sql: |
    SELECT
      book,
      desk,
      instrument_type,
      currency,
      SUM(notional) AS total_notional,
      SUM(CASE WHEN side = 'BUY' THEN quantity ELSE -quantity END) AS net_quantity,
      COUNT(*) AS trade_count,
      AVG(price) AS avg_price,
      MAX(trade_time) AS max_trade_time
    FROM slices
    GROUP BY book, desk, instrument_type, currency

# --- Window ---
window:
  interval_ms: 1000                   # Micro-batch window size (100-60000)
                                      # 100ms  — HFT, sub-second dashboards
                                      # 1000ms — default, trading desks, betting
                                      # 5000ms — IoT, manufacturing, e-commerce
                                      # 10000ms — adtech campaigns, low-frequency monitoring

# --- Compaction ---
# Background thread consolidates older slices into the main aggregate.
# No pauses on the hot path — compaction reads below the cutoff,
# hot path reads above it. One atomic integer separates them.
compaction:
  interval_windows: 60                # Compact every 60 windows (1 minute at 1s windows)

# --- Retention ---
# Raw slices are written to time-partitioned Parquet files by the compaction
# thread before being pruned from DuckDB. This provides historical drill-through
# and enables --rebuild from full history after config changes.
retention:
  duration: 6d                        # How long to retain Parquet partition files
  parquet_path: "/data/hydrocube/slices/"  # Directory for time-partitioned Parquet files

# --- Persistence ---
persistence:
  enabled: true
  path: "/data/hydrocube/positions.db"
  # Flush aggregate state to disk every N windows.
  flush_interval: 10

# --- Publication ---
# NATS publication is optional. If omitted, deltas are only delivered via SSE to browsers.
# Enable for system-to-system integration (downstream consumers, other services).
# publish:
#   nats:
#     url: "nats://localhost:4222"
#     subject_prefix: "cube.positions"  # Deltas published to cube.positions.{dimension_value}

# --- Web UI ---
ui:
  enabled: true
  port: 8080

# --- Auth ---
# Open source: no auth (localhost use) or basic auth.
# Enterprise: OIDC + HTTP session-based access control.
#
# auth:
#   type: oidc                          # none | basic | oidc
#   issuer: "https://login.microsoftonline.com/{tenant}/v2.0"
#   client_id: ${HYDROCUBE_OAUTH_CLIENT_ID}
#   client_secret: ${HYDROCUBE_OAUTH_CLIENT_SECRET}
#   # Map OIDC groups/claims to dimension-level access control
#   access_rules:
#     - group: "Trading - FX"
#       dimensions: {desk: ["FX", "FX_Options"]}
#     - group: "Trading - Rates"
#       dimensions: {desk: ["Rates"]}
#     - group: "Risk Management"
#       dimensions: {}                  # empty = all dimensions (full access)

# --- Logging ---
log_level: info                       # debug | info | warn | error
```

---

## Ingest

### Sources (v1.0)

#### Kafka

Consumer group subscription to a topic. Consumes in micro-batches aligned with the window interval. Commits offsets after successful aggregation, ensuring at-least-once processing. Supports JSON format in v1.0. The `rdkafka` C dependency is the only external system library in the build.

### Sources (v1.1)

#### NATS

Subscribe to a subject. Messages arrive as individual records. Each message is one row. Lightweight, zero-config, ideal for development and smaller deployments. No persistence guarantees — if HydroCube is down, messages are lost.

#### NATS JetStream

Persistent, replayable stream with exactly-once delivery semantics. HydroCube creates a durable consumer and acknowledges messages after successful window processing. On restart, consumption resumes from the last acknowledged position. Preferred over core NATS for production deployments.

#### AMQP 1.0 (IBM MQ, Azure Service Bus, ActiveMQ, Solace)

Connects via the AMQP 1.0 standard protocol using `fe2o3-amqp` — pure Rust, no vendor SDK required. This is the primary path for IBM MQ integration: MQ 9.x+ exposes AMQP 1.0 channels natively. Also covers Azure Service Bus (via AMQPS + SAS auth), Apache ActiveMQ, and Solace. One implementation, multiple enterprise brokers.

For IBM MQ specifically: the AMQP channel must be enabled on the queue manager (`DEF CHANNEL(AMQP.CHANNEL) CHLTYPE(AMQP)`). No MQ client libraries need to be installed on the HydroCube host.

#### Redis Streams

Consumer group subscription to a Redis stream. Lightweight alternative to Kafka for teams already running Redis. Supports consumer groups for load distribution and acknowledges messages after processing. The `redis` crate is pure Rust.

#### File Watch

Monitor a directory for new or modified files. On detection, read the file, insert rows into the buffer, and optionally move the source file to an archive directory. Useful for batch-to-real-time bridge scenarios where upstream systems drop files periodically. Combined with a Lua transform, this handles any legacy file format.

**Partial write safety:** Upstream systems should write to a temporary filename (e.g., `trades.csv.tmp`) and rename to the final name on completion. Atomic rename guarantees a complete file. The watcher's `pattern` config (e.g., `"*.csv"`) naturally ignores temp files. This convention should be documented for operators.

### Message Formats

Inbound messages are parsed according to `source.format` and mapped to the `schema.columns` definition (or passed to the transform pipeline if configured).

| Format | Version | Use Case | Notes |
|--------|---------|----------|-------|
| `json` | v1.0 | Most modern systems | Field names match column names. Missing fields -> NULL. Extra fields ignored. |
| `csv` | v1.1 | File watch, legacy feeds | Column order matches schema order. Header row configurable via `source.csv_header`. |
| `ndjson` | v1.1 | Streaming/log data | One JSON object per line. Same parsing as `json`. |
| `avro` | v1.1 | Enterprise Kafka | Requires `source.schema_registry` URL. Schema resolved at startup, cached. |

For proprietary or binary formats (FIX, FpML, SWIFT MT/MX, fixed-width mainframe extracts), use a Lua transform to decode into flat row tables. HydroCube does not build native parsers for domain-specific formats — Lua is the escape hatch.

### Buffer

Raw messages accumulate in a memory buffer during each window interval. At window close, the buffer is passed through the transform pipeline (if configured) and the resulting flat rows are bulk-inserted into DuckDB's `slices` table with a `_window_id` tag.

```
Buffer -> Transform Pipeline (Lua/SQL) -> INSERT INTO slices
```

The buffer is a pre-allocated `Vec<Vec<Value>>` sized to expected throughput. No allocation during hot path.

---

## Transform Pipeline

Messages from the source are often nested, encoded in proprietary formats, or need enrichment before they can be aggregated. The transform pipeline reshapes raw messages into flat rows that match the `schema.columns` definition.

### When No Transform Is Configured

Raw messages are mapped directly to schema columns by name (JSON) or position (CSV). Missing fields become NULL. Extra fields are ignored. This covers the simple case where the source format already matches the target schema.

### SQL Transforms

For structural reshaping — unnesting arrays, extracting nested fields, filtering — SQL runs against a `raw_buffer` temp table that contains the current window's raw messages.

```yaml
transform:
  - type: sql
    sql: |
      SELECT
        trade_id, book, desk, ccy, timestamp,
        unnest.tenor, unnest.delta, unnest.gamma
      FROM raw_buffer, UNNEST(tenors) AS unnest
```

This is the right choice for:
- Exploding nested arrays (tenor ladders, multi-leg trades)
- Filtering out irrelevant messages
- Type casting and column renaming
- DuckDB JSON path extraction for deeply nested payloads

The full power of DuckDB's SQL is available — `UNNEST`, `LATERAL`, JSON functions, `regexp_extract`, `CASE` expressions.

### Lua Transforms

For complex parsing, proprietary binary formats, and enrichment logic that SQL can't express. The Lua runtime is embedded in the binary (~300KB) with microsecond per-call overhead.

```yaml
transform:
  - type: lua
    script: transforms/flatten_ladder.lua
    function: transform_batch    # batched: receives array of messages per window
    init: init                   # optional: called once at startup
```

**Batch contract (preferred):**

The `transform_batch` function receives the entire window's messages as a Lua array and returns all output rows at once. This minimises Rust-Lua marshalling overhead — one round-trip per window instead of one per message.

```lua
-- Called once at startup (optional).
-- ctx provides read-only helpers for loading reference data.
function init(ctx)
  holidays = ctx.load_csv("config/holidays.csv")
  instruments = ctx.load_json("config/instruments.json")
end

-- Called once per window with all messages in the batch.
-- messages: array of Lua tables parsed from raw messages.
-- Returns: flat array of row tables matching schema columns.
function transform_batch(messages)
  local rows = {}
  for _, msg in ipairs(messages) do
    for _, t in ipairs(msg.tenors) do
      local inst = instruments[msg.instrument_id]
      if inst then
        table.insert(rows, {
          trade_id = msg.trade_id,
          book = msg.book,
          desk = msg.desk,
          ccy = msg.ccy,
          instrument_type = inst.type,
          tenor = t.tenor,
          delta = t.delta,
          gamma = t.gamma,
          timestamp = msg.timestamp
        })
      end
    end
  end
  return rows
end
```

**Per-message fallback:**

If the script exports `transform` but not `transform_batch`, the engine falls back to calling `transform(msg)` per message. A warning is logged at startup:

```
WARN [transform] Lua script uses per-message transform() — consider transform_batch() for better performance
```

The per-message contract is identical to the batch contract but receives a single message:

```lua
-- Called per message (fallback). Prefer transform_batch for production use.
function transform(msg)
  local rows = {}
  -- same logic as above, but for a single message
  return rows
end
```

**Key properties:**

- Lua never touches DuckDB. It receives data, returns data. HydroCube handles all database operations.
- The `init` function loads static reference data into Lua tables at startup — instrument classifications, holiday calendars, counterparty mappings. These are in-memory lookups, not database queries.
- Return `{}` to drop a message (cancelled trades, heartbeats, test data).
- One-to-many: one message can produce N rows (ladder explosion, multi-leg).
- Output is validated against `schema.columns` — mismatched keys are logged and the row is rejected.

### Composable Pipelines

Steps execute in order. The output of each step feeds the next. Mix Lua and SQL freely:

```yaml
transform:
  # Step 1: Lua decodes a proprietary binary format into JSON-like tables
  - type: lua
    script: transforms/decode_fpml.lua
    function: parse_batch

  # Step 2: SQL unnests and reshapes the decoded data
  - type: sql
    sql: |
      SELECT trade_id, book, leg.ccy, leg.notional
      FROM parsed, UNNEST(legs) AS leg
      WHERE leg.notional > 0

  # Step 3: Lua enriches with static reference data
  - type: lua
    script: transforms/enrich.lua
    function: classify_batch
```

This separation is powerful: Lua handles what SQL can't (binary decoding, complex business logic, external lookups), SQL handles what it's best at (relational reshaping, filtering, type casting). Each step is independently testable.

### Performance

Lua batch transform overhead per window is dominated by the Rust-Lua marshalling round-trip. For a window of 100K messages, expect ~50-200ms total (one marshalling round-trip plus Lua iteration). This is within the 1-second window budget but a meaningful fraction. For extremely high-throughput cubes (>500K msg/sec), keep Lua transforms minimal or prefer SQL.

Per-message fallback incurs ~5-20 microseconds per message including marshalling. For 100K messages, that's 0.5-2 seconds — potentially exceeding the window budget. Use `transform_batch` for production workloads.

SQL transforms run as a single DuckDB query against the `raw_buffer` table and execute in single-digit milliseconds regardless of message count (DuckDB's columnar engine shines here).

---

## Aggregation

### Design Principle: No Merge Semantics

Traditional real-time OLAP engines (ActivePivot, Druid) maintain merge strategies per aggregate function — SUM is additive, AVG needs weighted recalculation, COUNT DISTINCT is not mergeable at all. This creates complexity, special cases, and approximation errors.

HydroCube takes a fundamentally simpler approach: **never merge. Always recompute from slices.** DuckDB aggregates fast enough that recomputing from recent data every second is cheaper than maintaining merge logic.

The result: any SQL aggregate function works correctly — SUM, AVG, COUNT DISTINCT, MEDIAN, percentiles, arbitrary expressions. No special cases. No approximations. No merge strategy table.

### Two-Table Model

| Table | Contents | Owner |
|-------|----------|-------|
| `consolidated` | Fully aggregated snapshot of all data up to the compaction cutoff | Compaction thread (via DB manager) |
| `slices` | Raw inbound rows tagged with `_window_id` | Hot path writes, both read (via DB manager) |

The hot path and the compaction thread operate on **completely different rows** separated by an atomic cutoff window ID. The DB manager thread serialises all DuckDB access, and compaction breaks its work into discrete steps so the hot path can interleave between them.

### Hot Path (every 1 second)

At each window close, the hot path sends commands to the DB manager to insert new rows into `slices`, then computes the current aggregate.

**Step 1: Flush buffer to slices**

```sql
INSERT INTO slices
  SELECT *, {current_window_id} AS _window_id FROM buffer;
```

**Step 2: Read atomic cutoff**

```rust
let cutoff = COMPACTION_CUTOFF.load(Ordering::Acquire);
```

**Step 3a: Decomposable measures — fast path via consolidated + recent slices**

```sql
-- consolidated stores decomposed components:
-- _total_notional (SUM), _trade_count (COUNT), _price_sum (for AVG), etc.
SELECT
    book, desk, instrument_type, currency,
    SUM(_total_notional) AS total_notional,
    SUM(_net_quantity) AS net_quantity,
    SUM(_trade_count) AS trade_count,
    SUM(_price_sum) / NULLIF(SUM(_trade_count), 0) AS avg_price,
    MAX(_max_trade_time) AS max_trade_time
FROM (
    -- One row per group from consolidated snapshot
    SELECT * FROM consolidated
    UNION ALL
    -- Recent slices aggregated into same shape
    SELECT
        book, desk, instrument_type, currency,
        SUM(notional) AS _total_notional,
        SUM(CASE WHEN side = 'BUY' THEN quantity ELSE -quantity END) AS _net_quantity,
        COUNT(*) AS _trade_count,
        SUM(price) AS _price_sum,
        MAX(trade_time) AS _max_trade_time
    FROM slices
    WHERE _window_id > cutoff
    GROUP BY book, desk, instrument_type, currency
) combined
GROUP BY book, desk, instrument_type, currency;
```

**Step 3b: Non-decomposable measures — slice scan fallback**

```sql
-- COUNT DISTINCT, MEDIAN, percentiles query all retained slices directly
SELECT
    book, desk, instrument_type, currency,
    COUNT(DISTINCT counterparty) AS unique_counterparties,
    MEDIAN(price) AS median_price
FROM slices
GROUP BY book, desk, instrument_type, currency;
```

Results from 3a and 3b are joined by dimension key to produce the complete aggregate row.

The hot path only ever touches the `consolidated` snapshot (one row per group — small) plus a bounded number of recent slices (capped by compaction interval). For decomposable measures this is constant-time regardless of total data ingested.

**Non-decomposable measure performance note:** For non-decomposable measures, cost is proportional to the number of retained slices in DuckDB (between compaction cycles). With compaction running every 60 seconds at 100K msg/sec, that's ~6M rows — expect 20-80ms for MEDIAN or COUNT DISTINCT. This is acceptable for 1-second windows but will consume most of the budget for sub-200ms windows. A startup warning is logged when non-decomposable measures are configured with `window.interval_ms < 500`:

```
WARN [aggregate] Non-decomposable measures (MEDIAN, COUNT DISTINCT) with <500ms window may exceed timing budget
```

### Compaction Thread (background, interleaved via DB manager)

Periodically rebuilds `consolidated` from itself plus older slices, writes old slices to Parquet partition files, then atomically swaps via the DB manager. The hot path interleaves between compaction steps.

```
Compaction cycle (each step is a separate DB manager command):

  Step 1: Pick cutoff = current_window_id - safety_buffer

  Step 2: CREATE TABLE consolidated_new AS
       SELECT
         book, desk, instrument_type, currency,
         SUM(_total_notional) AS _total_notional,
         SUM(_net_quantity) AS _net_quantity,
         SUM(_trade_count) AS _trade_count,
         SUM(_price_sum) AS _price_sum,
         MAX(_max_trade_time) AS _max_trade_time
       FROM (
         SELECT * FROM consolidated
         UNION ALL
         SELECT
           book, desk, instrument_type, currency,
           SUM(notional), SUM(CASE WHEN side='BUY' THEN quantity ELSE -quantity END),
           COUNT(*), SUM(price), MAX(trade_time)
         FROM slices WHERE _window_id <= cutoff
         GROUP BY book, desk, instrument_type, currency
       ) GROUP BY book, desk, instrument_type, currency
       -- (~50ms, hot path can interleave before next step)

  Step 3: Export old slices to Parquet partition file
       COPY (SELECT * FROM slices WHERE _window_id <= cutoff)
       TO '/data/hydrocube/slices/2026-04-13/window_{range}.parquet'

  Step 4: Atomic swap (sent as DB manager Transaction — no interleaving):
       BEGIN;
       DROP TABLE consolidated;
       ALTER TABLE consolidated_new RENAME TO consolidated;
       DELETE FROM slices WHERE _window_id <= cutoff;
       COMMIT;

  Step 5: Update atomic cutoff:
       COMPACTION_CUTOFF.store(cutoff, Ordering::Release)

  Step 6: Prune Parquet partition files older than retention.duration
```

Note: `consolidated` stores decomposed components (`_price_sum`, `_trade_count`) rather than final computed values (`avg_price`). This allows the hot path's outer query to correctly combine consolidated with recent slices for decomposable measures.

### Why This Works Without Locks

The cutoff window ID is the single coordination point:

- **Compaction thread** owns all slices at or below the cutoff. It reads them, aggregates them into `consolidated_new`, exports them to Parquet, deletes them, and bumps the cutoff forward.
- **Hot path** owns all slices above the cutoff. It writes new slices and reads only recent ones.
- **No overlap.** The cutoff only moves forward. The hot path reads it at the start of each window cycle. If compaction bumps it mid-cycle, the hot path uses the old value — this is safe because it just means the hot path aggregates a few extra slices that compaction hasn't claimed yet.
- **DB manager serialises access.** All DuckDB operations flow through the manager thread, eliminating connection-level concurrency issues. The lock-free property is about the data partitioning, not the DuckDB connection.

```rust
// The concurrency model
static COMPACTION_CUTOFF: AtomicU64 = AtomicU64::new(0);

// Hot path reads
let cutoff = COMPACTION_CUTOFF.load(Ordering::Acquire);
// ... send aggregate query to DB manager with cutoff parameter

// Compaction thread writes (after DB manager confirms swap)
COMPACTION_CUTOFF.store(new_cutoff, Ordering::Release);
```

### Any Aggregate Function Works

Because compaction always recomputes from raw slice data, the `consolidated` table is always correct for every aggregate function — including COUNT DISTINCT, MEDIAN, and percentiles. No approximations.

The subtlety is in the **hot path**, which unions `consolidated` with recent slices. Some aggregations are decomposable (you can combine partial results) and some are not:

| Function | Decomposable? | Hot Path Strategy |
|----------|--------------|-------------------|
| `SUM(x)` | Yes | `SUM` of partial SUMs — use consolidated + recent slices |
| `COUNT(*)` | Yes | `SUM` of partial COUNTs — use consolidated + recent slices |
| `MIN(x)` / `MAX(x)` | Yes | `MIN`/`MAX` of partial results — use consolidated + recent slices |
| `AVG(x)` | Yes (via decomposition) | Consolidated stores `_sum` and `_count`; outer computes `SUM(_sum) / SUM(_count)` |
| `COUNT(DISTINCT x)` | **No** | Falls back to all retained slices (ignores consolidated for this measure) |
| `MEDIAN(x)` | **No** | Falls back to all retained slices |
| `QUANTILE_CONT(x, p)` | **No** | Falls back to all retained slices |
| `STDDEV(x)` / `VARIANCE(x)` | Yes (via decomposition) | Consolidated stores `_sum`, `_sum_sq`, `_count`; outer recomputes |
| `SUM(a) / SUM(b)` | Yes | Decompose into constituent SUMs |
| `STRING_AGG(x, ',')` | **No** | Falls back to all retained slices |

**For decomposable measures**, the hot path queries `consolidated UNION ALL recent_slices_aggregate` — touching only a small number of rows regardless of total data volume.

**For non-decomposable measures**, the hot path queries all retained raw slices directly. With compaction running every 60 seconds, the slice count is bounded by the compaction interval's worth of data.

The engine detects decomposability at startup by inspecting the configured measure expressions, and generates the appropriate hot path SQL for each category. Both strategies run in the same window cycle — the decomposable measures use the fast path, the non-decomposable measures use the slice scan. Results are joined by dimension key before delta detection.

### Aggregation SQL Generation

The engine generates all aggregation SQL from the YAML config at startup. The generated SQL is a standard GROUP BY query — readable by any data engineer. No proprietary query language. Logged at `debug` level for transparency.

For the consolidated + slices union, the engine generates two queries:
1. **Slice aggregation**: the user's dimensions and measures applied to `slices WHERE _window_id > cutoff`
2. **Combined aggregation**: merge the slice aggregation with `consolidated` using compatible column names

Both are generated once at startup and reused every window cycle with only the cutoff parameter changing.

---

## Delta Detection

After each window's aggregation (consolidated + recent slices), compare the new result against the previous window's result to identify which groups changed.

### Implementation

Maintain an in-memory hash map of `dimension_key -> row_hash`:

```rust
struct DeltaDetector {
    previous: HashMap<DimensionKey, u64>,  // dimension values -> hash of measure values
}
```

After aggregation:
1. Compute hash of each group's measure values
2. Compare against `previous` map
3. Groups where hash differs (or key is new) are deltas
4. Update `previous` with current state
5. Deleted groups (in previous but not current) are also deltas (with zero/null measures)

The hash comparison avoids row-by-row value comparison. A single `u64` hash per group keeps memory usage minimal even for large cubes.

### Cost

For a cube with 50,000 groups where 200 change per second, delta detection touches 50,000 hashes (one scan) and publishes 200 rows. This is sub-millisecond work.

---

## Publication

### NATS Topic Structure

Deltas are published to NATS subjects based on dimension values for system-to-system integration:

```
cube.{cube_name}.all                    -- All deltas (full cube)
cube.{cube_name}.book.{value}           -- Deltas for a specific book
cube.{cube_name}.desk.{value}           -- Deltas for a specific desk
cube.{cube_name}.instrument_type.{value} -- Deltas for a specific instrument type
```

The `all` subject receives every delta. Dimension-specific subjects receive only deltas matching that dimension value. A single delta row may be published to multiple subjects.

**NATS max_payload requirement:** Arrow IPC payloads can be large during burst scenarios (market open, mass repricing). HydroCube checks the connected NATS server's `max_payload` at startup and logs a warning if it's below 4MB:

```
WARN [publish] NATS max_payload is 1MB — burst delta batches may be rejected. Recommend setting max_payload to at least 4MB in NATS server config.
```

### Payload Format

Arrow IPC record batch. Each message contains:
- The changed rows only (not the full cube)
- All dimension columns plus all measure columns
- A `_delta_type` column: `upsert` or `delete`

Arrow IPC is chosen because:
- Perspective consumes it natively with zero deserialisation
- Columnar format is compact for numeric-heavy data
- No schema negotiation — schema is embedded in the IPC header

### SSE Browser Delivery

Browsers receive deltas via Server-Sent Events (SSE) instead of NATS WebSocket. This eliminates the `nats.ws` browser dependency and simplifies authentication — the HTTP session cookie handles auth, no NATS JWT token issuance needed.

```
Browser -> GET /api/stream?filter=desk.Equities (with session cookie)
  -> Axum SSE endpoint validates session and OIDC group permissions
  -> Server subscribes the connection to the tokio::broadcast channel
  -> Server filters deltas to only those permitted for this user's groups
  -> Deltas streamed as SSE events (Arrow IPC, base64-encoded)
```

```javascript
const eventSource = new EventSource("/api/stream?filter=desk.Equities");

eventSource.onmessage = (event) => {
    const arrowTable = loadArrowIPC(base64Decode(event.data));
    perspectiveViewer.update(arrowTable);
};
```

**Fan-out:** A `tokio::broadcast` channel receives each delta batch from the aggregation engine. Each SSE connection spawns a receiver that filters deltas based on the user's permitted dimensions and forwards matching deltas. For dozens to hundreds of concurrent viewers, this is trivial. High-fan-out deployments (1000+ viewers) can add a reverse proxy or CDN with SSE support.

### Fan-Out (NATS)

For system-to-system integration, NATS handles all fan-out. The aggregation engine publishes once per subject; NATS delivers to all subscribers. Adding external consumers does not increase load on the engine.

---

## Parquet Partition Retention

Raw slices are preserved in time-partitioned Parquet files for historical drill-through, `--rebuild` support, and backup.

### How It Works

The compaction thread, after aggregating old slices into `consolidated`, exports them to Parquet before deleting from DuckDB:

```
/data/hydrocube/slices/
  2026-04-07/window_000001-086400.parquet
  2026-04-08/window_086401-172800.parquet
  ...
  2026-04-13/window_518401-current.parquet
```

Daily partitions. Each compaction cycle appends to today's partition. A background cleanup prunes partition directories older than `retention.duration`.

### Benefits

- **`--rebuild` from history:** On config change, drop `consolidated` and rebuild from Parquet files. DuckDB scans Parquet natively and efficiently.
- **Historical drill-through:** Ad-hoc SQL can query Parquet files directly: `SELECT * FROM '/data/hydrocube/slices/2026-04-10/*.parquet' WHERE book = 'FX_SPOT_1'`
- **Backup/archival:** Parquet files are portable. Copy them to S3, GCS, or any object store for long-term retention beyond the configured duration.
- **Active/active HA:** See Multi-Cube Deployment section.

### DuckDB Working Set

DuckDB only holds the working set:
- `consolidated` — one row per group (small, fixed size)
- `slices` — raw rows between the compaction cutoff and current window (bounded by compaction interval)

Historical data lives in Parquet on disk. This keeps DuckDB memory usage constant regardless of how long the cube has been running.

---

## Persistence

### DuckDB On-Disk State

DuckDB supports persistent databases. HydroCube maintains:

| Table | Contents | Persistence |
|-------|----------|-------------|
| `slices` | Raw inbound rows with `_window_id` | Rolling — exported to Parquet and pruned by compaction |
| `consolidated` | Pre-aggregated snapshot up to compaction cutoff (one row per group) | Persistent — rebuilt by compaction thread |
| `_cube_metadata` | Cube config hash, last window ID, compaction cutoff | Persistent |

### Crash Recovery

On startup:
1. Open existing DuckDB file at `persistence.path`
2. Read `_cube_metadata` to verify config compatibility
3. `consolidated` is immediately available — the cube is live in seconds
4. Any slices above the stored cutoff are still present — the first hot path cycle picks them up and produces a correct aggregate
5. Resume ingestion from the last committed Kafka offset

No replay from source required. Compare this to ActivePivot, which must rebuild the entire cube from scratch on restart.

### Config Change Handling

If the cube config (dimensions, measures, schema) has changed since the last run, the `_cube_metadata` config hash will not match. HydroCube refuses to start and instructs the user to run `--rebuild`:

```
ERROR [startup] Config hash mismatch — cube schema has changed since last run.
       Run with --rebuild to reconstruct from retained Parquet history.
       WARNING: --rebuild drops consolidated state and reprocesses from Parquet files.
       Retained history covers 6 days. Data older than retention.duration is lost.
```

`--rebuild` drops `consolidated`, scans all retained Parquet files plus any remaining slices in DuckDB, rebuilds `consolidated` from scratch, and resumes normal operation. This takes seconds for typical retention volumes.

No attempt is made to detect "compatible" vs. "incompatible" changes. Any config change requires `--rebuild`. This is explicit, predictable, and matches operational expectations in regulated environments where changes go through a planned CR process.

---

## Web UI

### Stack

- **Minijinja** — Rust-native templating for the HTML shell
- **Bootstrap 5** — Layout and styling, with **Bootswatch Flatly** (light) and **Darkly** (dark) themes vendored
- **Perspective** — WASM pivot viewer, vendored
- **CodeMirror** (minimal) — SQL editor with syntax highlighting

All assets embedded in the binary via `include_bytes!` or `rust-embed`. Zero external dependencies at runtime.

### Page Layout

```
+----------------------------------------------------------------------+
|  HydroCube -- trading_positions              [sun/moon] [pause] [3/s]|
+----------------------------------------------------------------------+
|                                                                      |
|  +----------------------------------------------------------------+  |
|  |                                                                |  |
|  |                   Perspective Panel                            |  |
|  |                 (pivot table / chart)                          |  |
|  |                                                                |  |
|  |               Auto-updates via SSE                             |  |
|  |                                                                |  |
|  +----------------------------------------------------------------+  |
|                                                                      |
|  +----------------------------------------------------------------+  |
|  |  SQL Query                                      [Run] [Export] |  |
|  |  SELECT * FROM consolidated WHERE desk = 'Rates'              |  |
|  +----------------------------------------------------------------+  |
|                                                                      |
|  Status: 48,721 groups | 1,204 deltas/sec | 8.2ms agg | 3 viewers  |
+----------------------------------------------------------------------+
```

### HTTP Endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/` | GET | Main UI page |
| `/api/stream` | GET | SSE endpoint for real-time delta streaming (with optional `?filter=` parameter) |
| `/api/query` | POST | Execute ad-hoc SQL against DuckDB, return Arrow IPC |
| `/api/snapshot` | GET | Full current aggregate (consolidated + recent slices) as Arrow IPC (initial browser load) |
| `/api/schema` | GET | Cube schema and config metadata |
| `/api/status` | GET | Health check, ingest rate, aggregate stats |
| `/auth/login` | GET | Initiate OIDC login flow (Enterprise) |
| `/auth/callback` | GET | OIDC callback — exchange code for tokens, set session cookie |
| `/assets/*` | GET | Static files (Perspective WASM, Bootstrap, JS) |

### Initial Load Flow

1. Browser opens `/` — if auth is enabled, redirect to `/auth/login` -> OIDC provider
2. After OIDC callback, session cookie is set
3. JS fetches `/api/snapshot` to get the full current aggregate as Arrow IPC
4. Perspective viewer loads the snapshot
5. JS opens SSE connection to `/api/stream` (session cookie authenticates and scopes the connection)
6. Incoming deltas are applied to Perspective via `viewer.update()`

Without auth configured, steps 1-2 are skipped — browser connects directly.

### Theming

Two vendored Bootswatch themes toggled client-side:

| Theme | Bootswatch | Use Case |
|-------|-----------|----------|
| Light | Flatly | Standard office environments, printing, screen sharing |
| Dark | Darkly | Trading floors, low-light environments, personal preference |

Toggle via the sun/moon icon in the header bar. Preference stored in `localStorage`, persists across sessions. Both themes are vendored CSS — switching is a single stylesheet swap with no server call.

Perspective's own theming is synced to match — it supports a `theme` attribute that can be set to `Pro` (light) or `Pro Dark` (dark).

### Ad-Hoc Query

The SQL panel allows users to run arbitrary queries against the DuckDB instance:

```sql
-- Drill through to raw slices for a specific book
SELECT * FROM slices WHERE book = 'FX_SPOT_1' ORDER BY trade_time DESC LIMIT 100;

-- Custom aggregation not in the cube config
SELECT instrument, SUM(notional) FROM slices WHERE desk = 'Rates' GROUP BY 1 ORDER BY 2 DESC;

-- Inspect the consolidated aggregate directly
SELECT * FROM consolidated WHERE currency = 'USD' ORDER BY total_notional DESC;

-- Query historical data from Parquet partition files
SELECT * FROM '/data/hydrocube/slices/2026-04-10/*.parquet' WHERE book = 'FX_SPOT_1';
```

Results are returned as Arrow IPC and rendered in a secondary Perspective viewer below the main panel.

### Export (v1.1)

One-click export of whatever the user currently sees in a Perspective panel — including any pivots, filters, and sorts applied.

| Format | Method |
|--------|--------|
| CSV | Client-side via Perspective's `view.to_csv()` — instant, no server call |
| Excel (.xlsx) | Server-side via `/api/export` — POST the current view config, DuckDB generates the query, returns xlsx |
| Parquet | Server-side via `/api/export` — POST the current view config, DuckDB COPY to Parquet, returns file |

---

## Authentication and Access Control

### Tiers

| Tier | Auth Model |
|------|-----------|
| **Open Source** | None (localhost use) or basic auth (`auth.type: basic`) |
| **Enterprise** | OIDC/OAuth2 + HTTP session-based dimension-level access control |

### Open Source: Basic Auth

Optional username/password on the HTTP layer. Protects the UI and API endpoints.

```yaml
auth:
  type: basic
  username: admin
  password: ${HYDROCUBE_PASSWORD}
```

Implemented as Axum tower middleware. Sufficient for internal/dev use.

### Enterprise: OIDC + Dimension-Level Access Control (v1.1)

Full integration with corporate identity providers (Azure AD, Okta, Keycloak, etc.) via OpenID Connect. Cube dimensions become the access control model.

#### OIDC Flow

```
Browser -> GET /auth/login
  -> 302 Redirect to OIDC provider (Azure AD, Okta, etc.)
  -> User authenticates
  -> OIDC provider redirects to /auth/callback with auth code
  -> HydroCube exchanges code for ID token + access token
  -> Extract user identity and group memberships from token claims
  -> Set encrypted session cookie
  -> Redirect to /
```

Standard OAuth2 authorization code flow. The `oauth2` crate handles the protocol. HydroCube stores the session server-side (in-memory HashMap, bounded by max concurrent users).

#### SSE Access Control

The SSE endpoint (`/api/stream`) checks the session cookie, resolves the user's OIDC groups, and filters deltas based on the configured access rules:

```yaml
auth:
  type: oidc
  issuer: "https://login.microsoftonline.com/{tenant}/v2.0"
  client_id: ${HYDROCUBE_OAUTH_CLIENT_ID}
  client_secret: ${HYDROCUBE_OAUTH_CLIENT_SECRET}
  group_claim: groups
  access_rules:
    - group: "Trading - FX"
      dimensions: {desk: ["FX", "FX_Options"]}
    - group: "Trading - Rates"
      dimensions: {desk: ["Rates", "Rates_Exotics"]}
    - group: "Risk Management"
      dimensions: {}                  # empty = full access
    - group: "IT Support"
      dimensions: {}
      read_only: true                 # can view but not run ad-hoc SQL
```

**Dimension-based access control.** The cube dimensions (book, desk, currency) map to filter rules. A trader on the FX desk's SSE connection only receives deltas where `desk IN ('FX', 'FX_Options')`. The filtering happens server-side in the SSE handler — the client never sees restricted data.

**Multiple group memberships.** If a user is in both "Trading - FX" and "Risk Management", they get the union of all permitted dimensions (effectively full access in this case).

**API endpoint protection.** The session middleware also gates `/api/query` and `/api/snapshot`. The `read_only` flag restricts ad-hoc SQL execution.

#### Session Lifecycle

- Sessions are stored in-memory, bounded by max concurrent users
- Session timeout is configurable (default: 8 hours)
- On session expiry, the SSE connection closes and the browser redirects to `/auth/login`
- Server-side session eviction on logout or timeout

---

## Graceful Shutdown

On receiving SIGINT or SIGTERM, HydroCube executes a controlled shutdown sequence to prevent data loss and ensure clean state for restart.

### Shutdown Sequence

```
1. Stop accepting new messages
   - Close Kafka consumer (stop polling)
   - Signal ingest thread to drain

2. Flush current window buffer
   - Process the partial window (don't discard buffered messages)
   - Run transform pipeline on remaining buffer

3. Final aggregation cycle
   - Insert buffered rows into slices
   - Run aggregation + delta detection
   - Publish final deltas to NATS and SSE

4. Commit source offsets
   - Kafka: commit consumer offsets for the last fully processed window
   - (v1.1) JetStream: acknowledge last processed message
   - (v1.1) Redis Streams: acknowledge last processed message

5. Final compaction (optional, best-effort)
   - If pending slices exist, run one compaction cycle
   - Export remaining slices to Parquet partition

6. Flush DuckDB to disk
   - Ensure consolidated and _cube_metadata are persisted
   - Close DuckDB connection cleanly

7. Close connections
   - Disconnect from NATS
   - Close all SSE connections (clients will see EventSource close and can reconnect)
   - Shut down Axum HTTP server

8. Exit with code 0
```

### Timeout

The shutdown sequence has a configurable timeout (default: 30 seconds). If the sequence hasn't completed within the timeout, the process exits with code 0 anyway — the persistent DuckDB state and Parquet files ensure no data loss on next startup. A warning is logged:

```
WARN [shutdown] Graceful shutdown timed out after 30s — forcing exit. State is consistent for restart.
```

---

## Multi-Cube Deployment

Each cube is one process, one config file, one DuckDB database. To run multiple cubes:

```bash
hydrocube --config positions.yaml &
hydrocube --config risk_metrics.yaml &
hydrocube --config order_flow.yaml &
```

Or with containers:

```yaml
# docker-compose.yaml
services:
  positions:
    image: neuralchilli/hydrocube
    volumes:
      - ./positions.yaml:/etc/hydrocube/cube.yaml
      - ./data/positions:/data
    ports:
      - "8080:8080"

  risk_metrics:
    image: neuralchilli/hydrocube
    volumes:
      - ./risk_metrics.yaml:/etc/hydrocube/cube.yaml
      - ./data/risk:/data
    ports:
      - "8081:8080"

  order_flow:
    image: neuralchilli/hydrocube
    volumes:
      - ./order_flow.yaml:/etc/hydrocube/cube.yaml
      - ./data/orders:/data
    ports:
      - "8082:8080"
```

No coordination between cubes. No shared state. No cluster management. Each cube is isolated. If one crashes, the others are unaffected. Resource limits per cube via container constraints.

### Active/Active High Availability

Two cube instances consuming from the same Kafka topic with **different consumer group IDs** both receive the full message stream independently. Each builds its own `consolidated` table and Parquet history. A load balancer in front distributes browser clients.

```yaml
# cube-a.yaml
source:
  type: kafka
  group_id: "hydrocube-positions-a"

# cube-b.yaml
source:
  type: kafka
  group_id: "hydrocube-positions-b"
```

If one instance goes down, the other keeps serving. The failed instance restarts and resumes from its last committed Kafka offset. No shared state, no failover protocol, no leader election — the architecture's process isolation makes active/active a deployment pattern, not a code feature.

For shared Parquet storage (e.g., NFS or S3-FUSE), use instance-prefixed paths to avoid write conflicts:

```yaml
# cube-a.yaml
retention:
  parquet_path: "/data/hydrocube/slices/instance-a/"

# cube-b.yaml
retention:
  parquet_path: "/data/hydrocube/slices/instance-b/"
```

---

## Performance Targets

| Metric | Target |
|--------|--------|
| Ingest throughput | 100K+ messages/second per cube |
| Aggregation latency (decomposable) | <10ms for 1 second window (1M raw rows, 50K groups) |
| Aggregation latency (non-decomposable) | <80ms for 1 second window (bounded by compaction interval rows) |
| Delta detection | <1ms for 50K groups |
| Publish latency | <1ms per delta batch (Arrow IPC serialisation) |
| End-to-end (source -> browser) | <100ms (dominated by window interval) |
| Startup (cold, empty DB) | <1 second |
| Startup (warm, persistent DB) | <2 seconds (DuckDB file load) |
| Startup (--rebuild, 6 days Parquet) | <30 seconds (DuckDB Parquet scan + aggregation) |
| Memory per cube | <500MB for 1M retained rows + 50K aggregate groups |

### Bottleneck Analysis

The window interval (configurable, default 1 second) dominates end-to-end latency. Within each window cycle:

1. Buffer flush + transform pipeline: ~5-10ms for 100K rows (Lua batch: ~50-200ms, SQL: ~2ms)
2. Insert transformed rows to slices: ~3ms for 100K rows (bulk INSERT)
3. Aggregate consolidated + recent slices: ~5-10ms decomposable, ~20-80ms non-decomposable
4. Delta detection: <1ms (hash comparison)
5. Arrow IPC serialisation: <1ms for 200 changed groups
6. NATS publish: <1ms
7. SSE broadcast: <1ms

Total processing per window (decomposable only): ~15-25ms, leaving 975ms+ of headroom within the default 1-second window.

Total processing per window (with non-decomposable): ~50-100ms, leaving 900ms+ of headroom.

For shorter windows (100ms for HFT-style use cases), the budget is tighter. Decomposable-only cubes can achieve 100ms windows. Cubes with non-decomposable measures should use 500ms+ windows.

The compaction thread runs via the DB manager and its steps interleave with hot path work. A full compaction of 1M rows across 50K groups takes ~50ms for the aggregation step plus ~20ms for the Parquet export. The hot path may experience brief queuing delays (~50ms worst case) while a compaction step executes, but this is within the window budget.

As the cube runs longer, the hot path cost stays constant: it always reads one `consolidated` table (fixed size) plus a bounded number of recent slices (capped by compaction interval). **Aggregation cost is O(compaction_interval), not O(total_data_ingested).**

The actual bottleneck at scale will be message deserialisation during ingest — parsing JSON is expensive. For maximum throughput, prefer binary formats (Avro in v1.1, or a flat binary protocol via Lua).

---

## CLI Interface

```
hydrocube [OPTIONS]

OPTIONS:
  --config <PATH>         Path to cube YAML config [required]
  --validate              Validate config and exit
  --rebuild               Drop consolidated state, rebuild from Parquet history + remaining slices
  --snapshot <PATH>       Export current aggregate to Parquet file and exit
  --reset                 Clear all persistent state (DuckDB + Parquet) and start fresh
  --log-level <LEVEL>     Override config log level (debug|info|warn|error)
  --no-ui                 Disable web UI (headless aggregation + NATS publish only)
  --ui-port <PORT>        Override config UI port
  --version               Print version and exit
```

### Operational Commands

```bash
# Validate config before deploying
hydrocube --config cube.yaml --validate

# Export current state for debugging or downstream use
hydrocube --config cube.yaml --snapshot /tmp/aggregate_dump.parquet

# Rebuild after config change (planned CR)
hydrocube --config cube.yaml --rebuild

# Fresh start — clears everything
hydrocube --config cube.yaml --reset

# Headless mode for production (no browser UI, just aggregation + NATS)
hydrocube --config cube.yaml --no-ui
```

---

## Error Handling

### Ingest Failures

- **Source unavailable** (Kafka down): Retry with exponential backoff. Log warnings. UI shows disconnected status.
- **Malformed message**: Log warning with message sample, skip message, increment error counter. Never crash on bad input.
- **Schema mismatch**: Missing fields become NULL. Extra fields are ignored. Type coercion failure skips the message.

### Transform Failures

- **Lua runtime error**: Log the error with script name and message content sample. Skip the message, increment error counter. Never crash on a Lua error.
- **Lua batch timeout**: If `transform_batch` exceeds a configurable timeout (default: 80% of window interval), log a warning and process what's available. Consider switching to SQL transforms for this throughput level.

### Aggregation Failures

- **DuckDB error**: Log the generated SQL and DuckDB error. Skip the window. The `consolidated` table and existing slices are unaffected — next window retries with fresh data.
- **Out of memory**: DuckDB surfaces OOM clearly. Reduce compaction interval, or add container memory.

### Publication Failures

- **NATS unavailable**: Buffer deltas in memory (bounded queue). Reconnect with backoff. On reconnect, publish a full snapshot to resync subscribers.
- **SSE client disconnect**: Clean up the broadcast receiver. No impact on other clients or the engine.
- **Slow SSE client**: Drop buffered deltas for that client if the buffer exceeds a threshold. Client reconnects and fetches a fresh snapshot via `/api/snapshot`.

### Persistence Failures

- **Disk full**: Log error, continue operating in-memory only. Status endpoint reports persistence degraded.
- **Corrupt DB file**: Rename corrupt file, start fresh, log error prominently.
- **Parquet write failure**: Log warning, continue operating. Slices remain in DuckDB until next successful Parquet export. `--rebuild` history may have gaps.

---

## Observability

### Status Endpoint (`/api/status`)

```json
{
  "cube": "trading_positions",
  "status": "running",
  "uptime_seconds": 3621,
  "ingest": {
    "source": "kafka",
    "connected": true,
    "messages_received": 4821903,
    "messages_per_second": 1204,
    "errors": 12,
    "last_error": "malformed JSON at offset 42"
  },
  "aggregation": {
    "groups": 48721,
    "windows_processed": 3620,
    "last_window_ms": 8.2,
    "avg_window_ms": 7.1,
    "deltas_per_second": 204,
    "non_decomposable_measures": true,
    "last_non_decomposable_ms": 42.3
  },
  "compaction": {
    "cutoff_window_id": 3560,
    "pending_slices": 60,
    "last_compaction_ms": 42.1,
    "last_compaction_at": "2026-04-13T14:29:00Z",
    "last_parquet_export_at": "2026-04-13T14:29:00Z"
  },
  "persistence": {
    "enabled": true,
    "path": "/data/hydrocube/positions.db",
    "last_flush": "2026-04-13T14:30:00Z",
    "db_size_mb": 142,
    "consolidated_rows": 48721,
    "slice_rows": 72400
  },
  "retention": {
    "parquet_path": "/data/hydrocube/slices/",
    "duration": "6d",
    "oldest_partition": "2026-04-07",
    "total_parquet_size_mb": 2480
  },
  "subscribers": {
    "sse_clients": 3,
    "nats_subjects": 12
  }
}
```

### Logging

Structured logs to stderr. Timestamps, log level, component tag.

```
2026-04-13T14:30:01Z INFO  [ingest] Connected to kafka broker localhost:9092 topic=trades.executed group=hydrocube-positions
2026-04-13T14:30:01Z INFO  [persist] Loaded existing state: 48,721 groups from /data/hydrocube/positions.db
2026-04-13T14:30:01Z INFO  [ui] Web UI available at http://localhost:8080
2026-04-13T14:30:01Z INFO  [sse] SSE endpoint available at /api/stream
2026-04-13T14:30:02Z DEBUG [aggregate] Window 1: 1,204 rows -> 204 deltas in 8.2ms
2026-04-13T14:30:03Z DEBUG [aggregate] Window 2: 1,187 rows -> 198 deltas in 7.8ms
2026-04-13T14:30:03Z WARN  [ingest] Malformed message skipped: invalid JSON at offset 42
2026-04-13T14:30:62Z DEBUG [compact] Compaction cycle: 72,400 slices -> consolidated (48,721 groups) in 42.1ms
2026-04-13T14:30:62Z DEBUG [compact] Exported 72,400 rows to /data/hydrocube/slices/2026-04-13/window_003501-003560.parquet
```

---

## Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Language | Rust | Performance, safety, single binary |
| SQL engine | DuckDB (`duckdb` crate) | Aggregation, persistence, Arrow export |
| Messaging (Kafka) | `rdkafka` crate (feature-gated) | Kafka ingest — requires librdkafka C library |
| Serialisation | Arrow IPC (`arrow` crate) | Zero-copy data transfer to Perspective |
| Scripting | Lua 5.4 (`mlua` crate, vendored) | Transform pipeline, message enrichment |
| Web server | `axum` | Embedded HTTP for UI, API, and SSE |
| SSE | `axum::response::Sse` | Real-time delta streaming to browsers |
| Broadcast | `tokio::broadcast` | Fan-out of deltas to SSE connections |
| Auth | `oauth2` crate | OIDC/OAuth2 flow for Enterprise (v1.1) |
| Templating | Minijinja | HTML page generation |
| UI framework | Bootstrap 5 (vendored) | Page layout and styling |
| Visualisation | Perspective WASM (vendored) | Pivot tables, charts, filtering |
| Logging | `tracing` crate | Structured logging |

### v1.1 additions

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Messaging (NATS) | `async-nats` crate | NATS + JetStream ingest and delta publication |
| Messaging (AMQP) | `fe2o3-amqp` crate | IBM MQ, Azure Service Bus, ActiveMQ, Solace — pure Rust |
| Messaging (Redis) | `redis` crate | Redis Streams ingest — pure Rust |
| Schema Registry | `apache-avro` + `schema_registry_converter` | Confluent Schema Registry for Avro on Kafka |
| File watch | `notify` crate | File system watcher for file_watch source |
| Webhook | `reqwest` crate | HTTP client for alert webhooks |

### Build Philosophy: "Everything" Binary

HydroCube ships as a single binary with all features included. No feature-gating of Lua, UI, or auth at build time. The binary includes everything; features are enabled or disabled via YAML config at runtime.

The only exception is Kafka: `rdkafka` requires `librdkafka` (C dependency) and is feature-gated at build time for environments where this is problematic.

```toml
[features]
default = ["kafka"]
kafka = ["rdkafka"]
```

Lua adds ~300KB to the binary (vendored via `mlua`). Perspective WASM adds ~8MB. Auth adds negligible size. The total binary is ~20-25MB — small enough that the "everything" approach has no downside.

---

## Crate Dependencies (Key)

### v1.0

| Crate | Purpose |
|-------|---------|
| `duckdb` (bundled) | Core engine, aggregation, persistence |
| `rdkafka` (feature-gated) | Kafka consumer (requires librdkafka) |
| `async-nats` | NATS publication of deltas (system-to-system) |
| `arrow` | Arrow IPC serialisation for Perspective |
| `mlua` (vendored Lua 5.4) | Embedded Lua runtime for transform scripts |
| `axum` | HTTP server for web UI, API, and SSE |
| `axum-extra` | Cookie/session support for auth |
| `minijinja` | HTML templating |
| `serde` + `serde_yaml` | Config parsing |
| `serde_json` | JSON message parsing |
| `tokio` | Async runtime |
| `tracing` | Structured logging |
| `rust-embed` | Static asset embedding |
| `clap` | CLI argument parsing |

### v1.1

| Crate | Purpose |
|-------|---------|
| `fe2o3-amqp` | AMQP 1.0 client for IBM MQ, Azure Service Bus, ActiveMQ (pure Rust) |
| `redis` | Redis Streams client (pure Rust) |
| `apache-avro` | Avro deserialisation |
| `schema_registry_converter` | Confluent Schema Registry integration for Avro |
| `oauth2` | OIDC/OAuth2 authorization code flow |
| `notify` | File system watcher for file_watch source |
| `reqwest` | HTTP client for alert webhooks |

---

## Vendored Assets

The following are vendored into the repository and embedded in the binary at build time:

| Asset | Size (approx) | Source |
|-------|---------------|--------|
| Perspective WASM + JS | ~8MB compressed | `@finos/perspective` npm package |
| Bootstrap 5 CSS + JS | ~200KB compressed | `bootstrap` npm package |
| Bootswatch Flatly theme | ~20KB | `bootswatch` npm package (light mode) |
| Bootswatch Darkly theme | ~20KB | `bootswatch` npm package (dark mode) |
| CodeMirror (minimal, SQL mode) | ~150KB | `codemirror` npm package |

Total embedded assets: ~9MB. The binary with all assets will be approximately 20-25MB.

Assets are downloaded and committed via a `scripts/vendor.sh` script that pins exact versions.

---

## v1 Scope

### v1.0 — Core Loop

- **Source:** Kafka (JSON format)
- Transform pipeline: SQL transforms (UNNEST, filtering, reshaping)
- Transform pipeline: Lua scripting with batch contract (`transform_batch`) and per-message fallback
- Composable multi-step transforms (Lua + SQL chained)
- YAML-configured pure SQL aggregation (dimensions inferred from GROUP BY)
- Any DuckDB aggregate function (SUM, AVG, COUNT DISTINCT, MEDIAN, percentiles)
- Configurable window intervals (100ms to 60s) with performance warnings for non-decomposable + short windows
- DB manager thread for serialised DuckDB access
- Lock-free compaction (atomic cutoff, interleaved via DB manager)
- Delta detection and NATS publication
- Arrow IPC payloads with NATS max_payload startup check
- Parquet partition retention with configurable duration
- DuckDB persistence and crash recovery
- Config change detection with `--rebuild` from Parquet history
- **Web UI:** Single Perspective panel with dark/light theming (Bootswatch Darkly/Flatly)
- **Web UI:** SSE-based real-time delta delivery (replaces NATS WebSocket)
- **Web UI:** Ad-hoc SQL query panel with Parquet drill-through
- **Web UI:** Status bar (groups, deltas/sec, aggregation timing, viewers)
- Basic auth
- Graceful shutdown (flush buffer, final aggregation, commit offsets, persist state)
- CLI: --config, --validate, --rebuild, --reset, --snapshot, --no-ui, --log-level

### v1.1 — Breadth

- **Sources:** NATS, NATS JetStream, AMQP 1.0 (IBM MQ, Azure Service Bus, ActiveMQ), Redis Streams, file watch (with partial write safety documentation)
- **Formats:** CSV, NDJSON, Avro (with Confluent Schema Registry)
- Multi-panel layout (1/2/4 Perspective panels)
- Saved views with group-based sharing (stored in DuckDB)
- URL-encoded view state for lightweight sharing
- Freeze/snapshot for point-in-time investigation
- Update throttle per panel (real-time / 5s / 10s / 30s / paused)
- Export to CSV (client-side), Excel, Parquet (server-side)
- Threshold alerting with SQL conditions, cooldown, NATS and webhook channels
- Alert toast notifications in UI with click-to-filter
- OIDC + HTTP session-based dimension-level access control
- Alert status in UI and `/api/status`

### Out of Scope (v2+)

- Sliding windows (overlapping time-based aggregation)
- Protobuf message format
- AWS Kinesis / Azure Event Hubs / GCP Pub/Sub (cloud-native queues)
- Multiple cubes per process
- TLS for NATS and HTTP (use a reverse proxy for now)
- Cube-to-cube references (cross-cube joins)
- Historical replay / time-travel queries
- OpenLineage integration
- Snapshot export scheduling
- WASM transform plugins (Lua covers this for now)
- Cluster mode / multi-node (probably never — just use more containers)
- Arrow IPC chunking over NATS for burst scenarios
- Separate cadence for non-decomposable measures

---

## Licensing

Business Source License 1.1.

**Additional Use Grant:** Production use is free for organisations with annual revenue under £50M. Production use above this threshold requires a commercial licence.

**Pricing:** Commercial licences are priced as a percentage of equivalent enterprise OLAP tooling costs, negotiated per organisation.

**Change Date:** Three years after final release, converts to Apache 2.0.

The intent: anyone who can't afford ActivePivot uses HydroCube for free. Anyone who can afford ActivePivot pays a fraction of what they'd pay ActivViam.

---

## Project Structure

```
hydrocube/
├── Cargo.toml
├── DESIGN.md
├── LICENSE.md
├── README.md
├── CLAUDE.md
├── cube.example.yaml
├── scripts/
│   └── vendor.sh              # Download and pin vendored assets
├── static/
│   ├── perspective/            # Vendored WASM + JS
│   ├── bootstrap/              # Vendored CSS + JS
│   ├── bootswatch/
│   │   ├── flatly.min.css      # Light theme
│   │   └── darkly.min.css      # Dark theme
│   └── codemirror/             # Vendored SQL editor (minimal)
├── templates/
│   └── index.html.j2           # Minijinja template for the UI
├── src/
│   ├── main.rs
│   ├── cli.rs                  # Clap CLI args
│   ├── config.rs               # YAML config parsing and validation
│   ├── db_manager.rs           # Single-connection DuckDB manager thread (channel-based)
│   ├── engine.rs               # Aggregation orchestration (hot path)
│   ├── ingest/
│   │   ├── mod.rs              # Ingest trait + factory
│   │   ├── kafka.rs            # Kafka consumer (feature-gated)
│   │   └── parser.rs           # Message format parsing (JSON; CSV/NDJSON/Avro in v1.1)
│   ├── transform/
│   │   ├── mod.rs              # Transform pipeline orchestration
│   │   ├── sql.rs              # SQL transform step (DuckDB UNNEST, filtering)
│   │   └── lua.rs              # Lua transform step (mlua runtime, batch + fallback)
│   ├── aggregation/
│   │   ├── mod.rs
│   │   ├── sql_gen.rs          # Parse user SQL, generate hot path queries
│   │   ├── decompose.rs        # Detect measure decomposability, generate fast/fallback paths
│   │   └── window.rs           # Micro-batch window management
│   ├── compaction.rs           # Background compaction (interleaved via DB manager)
│   ├── retention.rs            # Parquet partition writing and pruning
│   ├── delta.rs                # Delta detection (hash-based)
│   ├── publish.rs              # NATS delta publication
│   ├── persistence.rs          # DuckDB flush, recovery, and config hash check
│   ├── shutdown.rs             # Graceful shutdown sequence
│   ├── auth/
│   │   ├── mod.rs              # Auth middleware dispatcher (none/basic; oidc in v1.1)
│   │   └── basic.rs            # Basic auth middleware
│   ├── web/
│   │   ├── mod.rs
│   │   ├── server.rs           # Axum HTTP server + router
│   │   ├── api.rs              # REST endpoints (query, status, snapshot)
│   │   ├── sse.rs              # SSE endpoint + tokio::broadcast fan-out
│   │   └── assets.rs           # Embedded static file serving
│   └── error.rs                # Error types
└── tests/
    ├── aggregation_test.rs
    ├── compaction_test.rs
    ├── transform_test.rs
    ├── delta_test.rs
    ├── db_manager_test.rs
    └── integration_test.rs
```

### v1.1 additions to src/

```
│   ├── ingest/
│   │   ├── nats.rs             # NATS + JetStream subscriber
│   │   ├── amqp.rs             # AMQP 1.0 (IBM MQ, Azure Service Bus, ActiveMQ)
│   │   ├── redis_streams.rs    # Redis Streams consumer
│   │   └── file_watch.rs       # Directory watcher
│   ├── alerting.rs             # Threshold evaluation, cooldown, NATS/webhook dispatch
│   ├── auth/
│   │   ├── oidc.rs             # OIDC flow (login, callback, session management)
│   │   └── access.rs           # Dimension-level access control from OIDC groups
│   ├── web/
│   │   ├── views.rs            # Saved view CRUD (DuckDB-backed)
│   │   ├── snapshots.rs        # Freeze/snapshot capture and retrieval
│   │   └── export.rs           # CSV/Excel/Parquet export
└── tests/
    ├── alerting_test.rs
    └── sse_test.rs
```

---

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Clean shutdown (SIGINT/SIGTERM) |
| 1 | Configuration error (invalid YAML, missing required fields) |
| 2 | Source connection failure (after retries exhausted) |
| 3 | Persistence failure (cannot open or create DuckDB file) |
| 4 | Port conflict (UI port already in use) |
| 5 | Config hash mismatch (run with --rebuild) |

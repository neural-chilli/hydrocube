# Aggregation and Compaction Design

**Date:** 2026-04-25
**Status:** Approved for implementation planning

---

## Philosophy

HydroCube is a fabric for SQL, Lua and execution hooks, collected and organised. It does one thing well: mid-level rollup with stable, predictable performance. Everything else — pivoting, drill-down, hierarchy navigation, column calculations — is delegated to Perspective's WASM engine.

**The engine infers nothing.** All aggregation logic, compaction logic, and lifecycle management is written by the user in SQL or Lua. The engine provides:

- Named execution hooks with declared triggers
- Placeholder tokens that expand at runtime
- A Rust-side identity cache for efficient amendment routing
- Correct delta detection and Perspective fan-out

Incorrect SQL produces wrong output. This is documented as a user responsibility, not an engine concern. The manual's cookbook section carries the weight of correctness guidance through worked examples.

---

## Core Data Model

### Three-tier aggregation

For append tables with high-volume bootstrap data, the engine supports a three-tier data model that keeps publish performance flat throughout the day regardless of message volume:

| Tier | Table | Written when | Size | Changes |
|---|---|---|---|---|
| 1 | `sod_aggregate` | Once, at startup or reset | O(dimension combinations) | Never during the day |
| 2 | `intraday_compacted` | Each compaction cycle (rebuilt) | O(dimension combinations) | Every compaction interval |
| 3 | `{table_name}` | Every message | O(rows since last compaction) | Bounded by compaction interval |

The user declares all three tables and writes SQL for each tier. The engine provides placeholder tokens so each SQL stage sees only the rows it should.

### Raw data is never deleted by compaction

Raw rows in append tables are never deleted by compaction. They are retained for drill-through queries and subject only to a separate `retention.raw.duration` policy. This separation means:

- Compaction advances `compaction_cutoff` — it does not delete rows
- `{table_name}` in publish SQL always expands to rows with `_window_id > compaction_cutoff`
- Drill-through queries the raw table directly, ignoring compaction state

### The compaction cutoff

`compaction_cutoff` is a global atomic (`u64`) representing the highest `_window_id` whose rows have been captured in compacted aggregates. Two special cases:

- After `startup_sql` completes: engine sets `compaction_cutoff = MAX(_window_id)` across all append tables
- After `compaction_sql` completes: engine advances `compaction_cutoff` to `{new_cutoff}`

Both advances are atomic. The publish loop always reads a consistent cutoff.

---

## Execution Hooks

Five named hook stages. All optional except `publish`.

| Hook | Trigger | Required | Purpose |
|---|---|---|---|
| `startup` | Once at engine start, before first publish | No | Bootstrap aggregate build, SOD data load |
| `compaction` | Every `compaction.interval` (wall-clock) | No | Fold pending raw rows into intraday compacted aggregate |
| `publish` | Every `window.interval_ms` | **Yes** | Delta-detected and pushed to clients |
| `snapshot` | Calendar cron expression(s) | No | Point-in-time capture for audit and explain |
| `reset` | Calendar cron expression(s) | No | Period boundary handling — midnight, month start, year start |
| `housekeeping` | Calendar cron expression(s) | No | Periodic maintenance — snapshot trimming, archive cleanup, vacuum |

Each hook may carry an optional Lua sub-step that runs before the SQL. Hooks without SQL may consist of Lua only.

### Startup sequence

```
1. Resolve paths for all sources with load: startup or load: reset
2. Load startup sources (in declared order, fully drained before next step)
3. Pre-populate identity cache: SELECT DISTINCT <dedup_key> FROM <each append table>
4. Run startup SQL (if declared)
5. Engine sets compaction_cutoff = MAX(_window_id) across all append tables
6. Open all non-startup ingest sources
7. Start publish loop
```

The publish loop does not start until step 7. Connected Perspective clients see a loading state until the first publish completes.

### Reset sequence

```
1. Pause publish loop
2. Resolve paths for all sources with load: reset
3. Load reset sources (fully drained)
4. Re-populate identity cache from tables
5. Run reset SQL
6. Engine sets compaction_cutoff = MAX(_window_id)
7. Resume publish loop
```

If reset SQL fails: the previous period's aggregates remain in place, a warning is logged, and the engine continues serving the previous state rather than starting in a broken new-period state. The failure is surfaced via the `/api/status` endpoint.

---

## Placeholder Tokens

Tokens expand at the time the SQL executes, not at config parse time.

| Token | Available in | Expands to |
|---|---|---|
| `{table_name}` (append) | publish, snapshot | `table_name WHERE _window_id > {compaction_cutoff}` |
| `{pending.table_name}` | compaction | `table_name WHERE _window_id > {compaction_cutoff} AND _window_id <= {new_cutoff}` |
| `{new_cutoff}` | compaction | The window ID being set as the new compaction boundary |
| `{cutoff}` | all SQL | Current compaction cutoff (numeric) |
| `{table_name}` (replace/reference) | all SQL | Full table, all rows |
| `{path.source_name}` | startup, reset SQL | Resolved file path(s) from named source's path resolver |
| `{publish_sql}` | snapshot | Full publish SQL as a subquery, with all inner tokens resolved at execution time |
| `{now}` | all SQL | Current wall-clock timestamp |
| `{period_start}` | all SQL | Start of current calendar period (if reset is declared) |
| `{YYYY}` `{MM}` `{DD}` `{date}` `{YYYYMMDD}` | paths, all SQL | Current date components at time of execution |

When `{publish_sql}` appears in snapshot SQL, all placeholder tokens within the publish SQL are resolved at snapshot execution time — the expansion is inside-out.

---

## Config Schema

### Top-level structure

```yaml
name: string                    # cube name
description: string             # optional

tables: [...]                   # table declarations
sources: [...]                  # ingest source declarations

window:
  interval_ms: 1000             # publish interval, 100-60000ms

persistence:
  enabled: true
  path: /data/hydrocube/cube.db
  flush_interval: 10            # seconds

retention:
  raw:
    duration: 7d                # keep raw slices for drill-through (d/h suffix)
    parquet_path: /data/archive/ # optional — export before dropping
  aggregates:
    duration: forever           # or Nd/Nh

drillthrough:
  max_rows: 50000               # default, user-configurable

delta:
  epsilon: 0.0                  # suppress updates smaller than this (0 = exact comparison)

aggregation:
  key_columns: [col1, col2]     # required — stable row identity for delta detection
  dimensions:  [col1, col2]     # optional — for drill-across declarations
  measures:    [col3, col4]     # optional — documentation and tooling only

  startup:
    lua: { function: fn, inline: | ... }   # optional
    sql: |
      ...

  compaction:
    interval: 60s
    lua: { function: fn, inline: | ... }   # optional
    sql: |
      ...

  publish:
    sql: |
      ...

  snapshots:
    - name: snapshot_name
      schedule: "cron expression"
      lua: { function: fn, inline: | ... }   # optional
      sql: |
        ...

  reset:
    schedule: "cron expression"
    lua: { function: fn, inline: | ... }    # optional
    sql: |
      ...

  housekeeping:
    - name: job_name
      schedule: "cron expression"
      sql: |
        ...

  reaggregation:
    schedule: "cron expression"             # optional nightly rebuild
```

### Table declaration

```yaml
tables:
  - name: trades
    mode: append                 # append | replace | reference
    event_time_column: trade_time  # optional, for event-time windowing
    key_columns: [book, instr]   # required for replace tables
    schema:
      columns:
        - { name: col, type: DUCKDB_TYPE }
```

### Source declaration

```yaml
sources:
  - name: source_name            # required if referenced via {path.source_name}
    type: kafka | nats | http | file
    table: table_name
    format: json | csv | parquet
    load: startup | reset        # omit for continuous sources

    # File sources
    path: /data/{date}/file.ext  # static path with optional date tokens
    path_resolver:               # mutually exclusive with path:
      function: fn_name
      inline: |
        function fn_name(ctx)
          -- ctx.today_iso, ctx.YYYY, ctx.MM, ctx.DD, ctx.YYYYMMDD available
          return { "/data/" .. ctx.YYYYMMDD .. "/file.parquet" }
        end
      # script: external_file.lua  # alternative to inline

    batch_size: 500              # exact batch size for file sources

    # Kafka sources
    brokers: ["host:9092"]
    topic: topic.name
    group_id: group-id
    max_batch_size: 200          # ceiling — actual batch may be smaller
    batch_wait_ms: 50            # wait up to this long before flushing to Lua

    # NATS sources
    url: nats://host:4222
    subject: "subject.>"

    # Lua transform (all source types)
    transform:
      - type: lua
        function: fn_name
        inline: |
          function fn_name(batch)
            -- batch is a table of messages
            -- return a flat table of rows
          end
        # script: external_file.lua  # alternative to inline

    # Identity-based amendment routing (append tables only)
    identity_key: trade_id       # column whose value identifies a unique entity
```

### Lua inline vs external script

Every Lua declaration accepts either `inline:` (YAML multiline string) or `script:` (path to external file). Declaring both is a startup validation error. `inline:` is preferred for self-contained cubes. `script:` is preferred for large transforms shared across cubes.

---

## Lua Contract

### Batch contract

All Lua functions receive a **batch** (table of messages or rows) and return a **flat table of output rows**. This amortises the Lua/Rust boundary cost across many messages per call. A batch of N messages may return M rows where M ≠ N (explosion, filtering, or both).

```lua
function transform(batch)
  local out = {}
  for _, msg in ipairs(batch) do
    -- process msg, append zero or more rows to out
    out[#out + 1] = { ... }
  end
  return out
end
```

### Query context

Transform functions optionally accept a second argument — a read-only query context. This enables amendment routing without a full DuckDB round-trip per message.

```lua
function transform(batch, ctx)
  -- ctx.query(sql, ...params) → list of row tables
  -- Read-only. Parameters are positional (?).
  -- Goes through DbManager channel — use sparingly on hot paths.
  local rows = ctx.query(
    "SELECT SUM(pv01) AS net FROM greeks WHERE trade_id IN (?,?)",
    id1, id2
  )
  return rows[1].net
end
```

**Use condition:** `ctx.query` is appropriate for amendment handling where amendments are rare relative to new bookings. It is not appropriate for per-message enrichment at high throughput. Queries are serialised through the DbManager channel and compete with the publish query. The spec recommends batching all lookups for a single batch into one query (see amendment handling section).

---

## Identity Cache and Amendment Routing

### The problem

When a trade is amended and the upstream sends a full replacement (not a reversal), the old trade's contribution is already baked into compacted aggregates. Lua cannot cheaply determine whether a message represents a new trade or an amendment without knowing whether the trade has been seen before.

### The identity cache

A Rust-side `HashSet<String>` keyed on the value of `identity_key` for each append table source. Characteristics:

- **Unbounded** — no LRU, no eviction. A bank's entire trade population fits comfortably: 10 million trade IDs at ~50 bytes each is ~500MB.
- **Pre-populated at startup** — before the ingest path opens, the engine queries `SELECT DISTINCT {identity_key} FROM {table}` and loads all known IDs. This ensures no known trade is misclassified as new after a restart.
- **Exposed to Lua** via `ctx.seen(table, key)` and `ctx.mark_seen(table, key)`.

### Amendment routing rule

```
trade identity in cache?
├── YES → slow path: ctx.query to get net contribution, emit synthetic reversal + new rows
└── NO  → fast path: emit new rows directly, ctx.mark_seen(table, trade_id)
```

### Why this handles redeliveries too

When Kafka redelivers a message, the trade_id is in the identity cache (it was added when first processed). The slow path fires:

- DuckDB lookup: returns current net contribution (e.g. pv01 = +1500)
- Synthetic reversal emitted: pv01 = −1500
- Original content re-emitted: pv01 = +1500
- Net aggregate change: 0 ✓

Correct behaviour without a separate dedup cache. The slow path is always safe — it produces the correct aggregate regardless of whether the trigger was an amendment or a redelivery. Since redeliveries are rare, the occasional unnecessary DuckDB lookup is acceptable.

### Batching slow-path lookups

When a batch contains multiple amendments, all lookups are batched into a single DuckDB query:

```lua
function handle_greeks(batch, ctx)
  -- First pass: collect all trade IDs needing lookup
  local to_lookup = {}
  for _, msg in ipairs(batch) do
    if ctx.seen("greeks", msg.trade_id) then
      to_lookup[#to_lookup + 1] = msg.trade_id
    end
  end

  -- One query for the entire batch
  local net = {}
  if #to_lookup > 0 then
    local ph = string.rep("?,", #to_lookup):sub(1, -2)
    local rows = ctx.query(
      "SELECT trade_id, curve_id, tenor, SUM(pv01) AS net " ..
      "FROM greeks WHERE trade_id IN (" .. ph .. ") " ..
      "GROUP BY trade_id, curve_id, tenor",
      table.unpack(to_lookup)
    )
    for _, r in ipairs(rows) do
      net[r.trade_id .. "|" .. r.curve_id .. "|" .. r.tenor] = r.net
    end
  end

  -- Second pass: emit reversals and new rows
  local out = {}
  for _, msg in ipairs(batch) do
    if ctx.seen("greeks", msg.trade_id) then
      for _, e in ipairs(msg.sensitivities or {}) do
        local key = msg.trade_id .. "|" .. e.curve_id .. "|" .. e.tenor
        local n = net[key]
        if n and n ~= 0 then
          out[#out + 1] = { trade_id=msg.trade_id, book=msg.book, desk=msg.desk,
            ccy=msg.ccy, curve_id=e.curve_id, tenor=e.tenor, pv01=-n,
            booking_time=msg.booking_time }
        end
      end
    else
      ctx.mark_seen("greeks", msg.trade_id)
    end
    if msg.amendment_type ~= "CANCEL" then
      for _, e in ipairs(msg.sensitivities or {}) do
        out[#out + 1] = { trade_id=msg.trade_id, book=msg.book, desk=msg.desk,
          ccy=msg.ccy, curve_id=e.curve_id, tenor=e.tenor, pv01=e.pv01,
          booking_time=msg.booking_time }
      end
    end
  end
  return out
end
```

### Amendment option summary

| Situation | Recommended approach |
|---|---|
| Upstream sends explicit reversal + rebooking | Option A — no engine overhead, SUM handles it naturally |
| Mutable current state, history not required | Option B — replace table keyed on entity ID |
| Full replacement from upstream, low amendment rate | Option D — identity cache + synthetic reversal via Lua |
| Full replacement from upstream, high amendment rate | Option B — replace table is the correct abstraction |
| Data correction discovered after the fact | Option C — `POST /api/reaggregate` |

**Fundamental constraint:** Append tables with pre-aggregated tiers are correct only for immutable events or reversal-pattern amendments. Trying to use them for high-rate mutable data without reversals is fighting the architecture. The replace table exists for this case.

---

## Delta Detection

The engine compares each `publish_sql` result to the previous result using `key_columns` as stable row identity. Three signal types are emitted to Perspective clients:

- **INSERT** — key present in current result, absent in previous
- **UPDATE** — key present in both, at least one measure value differs
- **DELETE** — key present in previous, absent in current

A synthetic `_key` column (`CONCAT_WS('|', key_col1, key_col2, ...)`) is appended to the publish result for use as the Perspective row index.

### Float comparison

Exact comparison by default. A per-cube `delta.epsilon` setting suppresses UPDATE signals where `|new − old| < epsilon` for all measures. Recommended for P&L and risk cubes where floating-point rounding can produce spurious deltas.

### Reset handling

When `reset_sql` fires, the engine clears the in-memory previous-result snapshot before the next publish. The first post-reset publish emits INSERT for all rows in the new result — no stale DELETEs, no phantom UPDATEs.

### Large result set warning

If `publish_sql` produces more than a configurable threshold (default 10,000 rows), the engine logs a warning on the first occurrence. The publish continues — this is advisory. Repeated warnings indicate the aggregate grain is wrong for the use case.

---

## Drill-Through

Raw append table rows are retained for drill-through queries. The endpoint defined in the drill-through spec is extended with an explicit row limit:

```
GET /api/drillthrough/{table_name}?{dimension_filters}&limit=50000
```

Default limit: 50,000 rows. Configurable via `drillthrough.max_rows`. Returns HTTP 413 with a plain-language message if exceeded.

**What is drillable:** raw rows within the raw retention window. Pre-aggregated tiers (`sod_aggregate`, `intraday_compacted`) are not drillable — source records no longer exist at that grain. This is a documented constraint, not a bug.

**Replace and reference tables:** returns HTTP 400. Current-state tables carry no history.

---

## Re-aggregation

When source data is corrected, aggregates built from bad data can be rebuilt by re-running `startup_sql` against the current raw table state.

**Trigger:**
```
POST /api/reaggregate    — immediate
```
Or via config:
```yaml
aggregation:
  reaggregation:
    schedule: "0 2 * * *"   # nightly rebuild
```

**Sequence:**
1. Suspend compaction schedule
2. Re-run `startup_sql` (rebuilds SOD aggregate from all current raw rows)
3. Truncate `intraday_compacted`
4. Advance `compaction_cutoff = MAX(_window_id)`
5. Resume compaction and publish

The live view continues serving during re-aggregation. The old SOD aggregate stays in place until `CREATE OR REPLACE TABLE` completes atomically. The inconsistency window equals the duration of `startup_sql`.

**Retention constraint:** re-aggregation is only correct within the raw retention window. Raw rows older than `retention.raw.duration` may have been deleted. For full historical re-aggregation, `startup_sql` must be written to also read from the Parquet archive — this is the user's responsibility.

**Cubes without `startup_sql`:** `POST /api/reaggregate` returns 200 OK with no-op.

---

## File Sources and Path Resolution

### Path declaration

Every file source specifies its path one of two ways — mutually exclusive:

```yaml
path: /data/{YYYYMMDD}/file.parquet    # static path with date tokens

path_resolver:                          # Lua function returning path(s)
  function: fn_name
  inline: |
    function fn_name(ctx)
      return { "/data/" .. ctx.YYYYMMDD .. "/file.parquet" }
    end
```

Date tokens (`{YYYY}`, `{MM}`, `{DD}`, `{date}`, `{YYYYMMDD}`) expand to the current date at the time the path is resolved — the start of each startup or reset cycle.

### Multi-file resolution

The path resolver may return a list of paths. The engine reads each in declared order. Multiple paths behave identically to a single path — all rows are inserted into the same table with sequentially assigned `_window_id` values.

For DuckDB's `parquet_scan`, `{path.source_name}` expands to a DuckDB list literal when multiple paths are returned:

```sql
FROM parquet_scan({path.greeks_sod})
-- expands to: FROM parquet_scan(['/data/20260425/file1.parquet', '/data/20260425/file2.parquet'])
```

### load: startup vs load: reset

- `load: startup` — source runs once at engine start, before `startup_sql`
- `load: reset` — source runs at engine start AND at each reset cycle, before reset SQL
- Neither — source is a continuous ingest source, starts after `startup_sql` completes

A source may declare at most one `load:` value.

### Parquet SOD loads without a file source

If the SOD file is already at the correct grain (no Lua explosion needed), it can be read directly in `startup_sql` via DuckDB's native `parquet_scan`. No file source or Lua is required:

```yaml
aggregation:
  startup:
    sql: |
      CREATE OR REPLACE TABLE sod_greeks AS
      SELECT desk, book, ccy, curve_id, tenor, SUM(pv01) AS total_pv01
      FROM parquet_scan('{path.greeks_sod}')
      GROUP BY desk, book, ccy, curve_id, tenor;
```

This is the fastest SOD load path — no row-by-row JSON parsing, no Lua invocation, DuckDB reads columnar Parquet at near-memory-bandwidth.

---

## Housekeeping

A list of named, cron-scheduled SQL jobs that run outside the publish cycle. Intended for periodic maintenance: trimming snapshot tables, deleting archived rows, vacuuming, or any other DuckDB DDL/DML that should run on a schedule without producing a publish output.

```yaml
aggregation:
  housekeeping:
    - name: trim_eod_snapshots
      schedule: "0 2 * * *"          # 02:00 daily
      sql: |
        DELETE FROM eod_risk_snapshots
        WHERE snap_time < NOW() - INTERVAL 90 DAYS;

    - name: trim_period_snapshots
      schedule: "0 3 1 * *"          # 03:00 on the 1st of each month
      sql: |
        DELETE FROM period_snapshots
        WHERE period_end < NOW() - INTERVAL 2 YEARS;
```

Housekeeping jobs run asynchronously from the publish loop — they do not pause or block publishing. If a housekeeping job fails, the error is logged and surfaced at `/api/status`; the engine continues running. All placeholder tokens (`{now}`, `{YYYY}`, etc.) are available in housekeeping SQL.

Snapshot tables were previously trimmed inline at the top of snapshot SQL. Housekeeping is the preferred location — it keeps snapshot SQL focused on capturing and keeps maintenance logic on a separate, independently-scheduled clock.

---

## Snapshot Table Retention

Snapshot SQL appends rows to a named table indefinitely. The recommended approach is to trim via a housekeeping job on a separate schedule rather than inside the snapshot SQL itself. Both approaches work:

```sql
-- Option A: trim inside snapshot SQL (simple, runs on snapshot schedule)
DELETE FROM eod_risk_snapshots
WHERE snap_time < NOW() - INTERVAL 90 DAYS;

INSERT INTO eod_risk_snapshots
SELECT CURRENT_TIMESTAMP AS snap_time, * FROM ({publish_sql}) s;
```

```yaml
# Option B: trim via housekeeping (preferred — independent schedule)
housekeeping:
  - name: trim_eod_snapshots
    schedule: "0 2 * * *"
    sql: |
      DELETE FROM eod_risk_snapshots
      WHERE snap_time < NOW() - INTERVAL 90 DAYS;
```

---

## Startup Failure Handling

If `startup_sql` fails: the engine refuses to start and logs a clear error with the failure reason. It does not start publishing with partially-built aggregate tables.

If a reset cycle's SQL fails: the previous period's aggregates remain in place. The engine logs a warning, continues serving the previous state, and surfaces the failure at `/api/status`. It does not start a new period in a broken state.

In both cases, the engine never publishes from an intermediate state.

---

## Schema Change Detection

The existing `schema_hash()` mechanism is extended to cover all fields that invalidate persisted state. Changes to any of the following require `--rebuild`:

- `schema.columns` on any table
- `aggregation.key_columns`
- `aggregation.publish.sql`
- `aggregation.startup.sql`

Changes to compaction SQL, snapshot SQL, reset SQL, source configuration, and Lua scripts do not require `--rebuild` — they take effect on the next engine start.

---

## Configuration Validation

The engine validates the following at startup and refuses to start on error:

- `publish.sql` is declared
- `key_columns` are non-empty
- All columns named in `key_columns` are present in `publish_sql` output (validated by running a `DESCRIBE` on the SQL)
- `{path.source_name}` references in SQL have a corresponding named source
- `inline:` and `script:` are not both declared on the same Lua block
- Sources with `path_resolver` have a `name:` declared
- Replace tables have `key_columns` declared
- `window.interval_ms` is between 100 and 60,000
- `batch_wait_ms` does not exceed `window.interval_ms / 2`

---

## Five Pattern Cookbook

### Pattern 1 — Day-population cumulative

**Use case:** Millions of trades bootstrapped from upstream at start of day. Hundreds of new trades per second intraday. Users want notional, net quantity, and trade count by book / desk / instrument type.

```yaml
name: trade_positions
description: Intraday cumulative position aggregation

tables:
  - name: trades
    mode: append
    schema:
      columns:
        - { name: trade_id,        type: VARCHAR }
        - { name: book,            type: VARCHAR }
        - { name: desk,            type: VARCHAR }
        - { name: instrument_type, type: VARCHAR }
        - { name: currency,        type: VARCHAR }
        - { name: notional,        type: DOUBLE }
        - { name: quantity,        type: DOUBLE }
        - { name: side,            type: VARCHAR }

sources:
  - name: trades_sod
    type: file
    table: trades
    format: json
    load: reset
    path_resolver:
      function: resolve_sod
      inline: |
        function resolve_sod(ctx)
          return { "/data/trades/" .. ctx.YYYYMMDD .. "_population.json" }
        end
    batch_size: 500
    transform:
      - type: lua
        function: normalize
        inline: |
          function normalize(batch)
            return batch   -- already flat, pass through
          end

  - name: trades_intraday
    type: kafka
    brokers: ["localhost:9092"]
    topic: trades.executed
    group_id: hydrocube-positions
    table: trades
    format: json
    max_batch_size: 200
    batch_wait_ms: 50
    identity_key: trade_id

window:
  interval_ms: 1000

retention:
  raw:
    duration: 2d
  aggregates:
    duration: forever

drillthrough:
  max_rows: 50000

aggregation:
  key_columns:  [book, desk, instrument_type, currency]
  dimensions:   [book, desk, instrument_type, currency]
  measures:     [total_notional, net_quantity, trade_count]

  startup:
    sql: |
      CREATE OR REPLACE TABLE sod_aggregate AS
      SELECT book, desk, instrument_type, currency,
        SUM(notional)                                                AS total_notional,
        SUM(CASE WHEN side = 'BUY' THEN quantity ELSE -quantity END) AS net_quantity,
        COUNT(*)                                                     AS trade_count
      FROM trades
      GROUP BY book, desk, instrument_type, currency;

      CREATE OR REPLACE TABLE intraday_compacted
        AS SELECT * FROM sod_aggregate WHERE false;

  compaction:
    interval: 60s
    sql: |
      CREATE OR REPLACE TABLE intraday_compacted AS
      SELECT book, desk, instrument_type, currency,
        SUM(total_notional) AS total_notional,
        SUM(net_quantity)   AS net_quantity,
        SUM(trade_count)    AS trade_count
      FROM (
        SELECT * FROM intraday_compacted
        UNION ALL
        SELECT book, desk, instrument_type, currency,
          SUM(notional)                                                AS total_notional,
          SUM(CASE WHEN side = 'BUY' THEN quantity ELSE -quantity END) AS net_quantity,
          COUNT(*)                                                     AS trade_count
        FROM {pending.trades}
        GROUP BY book, desk, instrument_type, currency
      ) t
      GROUP BY book, desk, instrument_type, currency;

  publish:
    sql: |
      SELECT book, desk, instrument_type, currency,
        SUM(total_notional) AS total_notional,
        SUM(net_quantity)   AS net_quantity,
        SUM(trade_count)    AS trade_count
      FROM (
        SELECT * FROM sod_aggregate
        UNION ALL
        SELECT * FROM intraday_compacted
        UNION ALL
        SELECT book, desk, instrument_type, currency,
          SUM(notional)                                                AS total_notional,
          SUM(CASE WHEN side = 'BUY' THEN quantity ELSE -quantity END) AS net_quantity,
          COUNT(*)                                                     AS trade_count
        FROM {trades}
        GROUP BY book, desk, instrument_type, currency
      ) t
      GROUP BY book, desk, instrument_type, currency

  reset:
    schedule: "0 7 * * 1-5"
    sql: |
      DROP TABLE IF EXISTS sod_aggregate;
      DROP TABLE IF EXISTS intraday_compacted;
```

**Note on amendments:** `trade_count` uses `COUNT(*)`. If amendments arrive as reversals, use `SUM(trade_count_contribution)` where new bookings carry `+1` and cancellations carry `−1`. `COUNT(*)` counts every row including reversal rows.

---

### Pattern 2 — Real-time risk (greeks + market data)

**Use case:** Trade PV01 sensitivities bootstrapped from file at start of day. New trade greeks arrive as sensitivity messages with delta ladders (one message per trade, N tenor entries). Live market data tick feed. Users want real-time `SUM(pv01 × move_bps)` by desk, book, currency, and tenor. EOD snapshot for explain activity.

```yaml
name: rates_risk
description: Real-time PV01 risk by desk, book, currency and tenor

tables:
  - name: greeks
    mode: append
    schema:
      columns:
        - { name: trade_id,     type: VARCHAR }
        - { name: book,         type: VARCHAR }
        - { name: desk,         type: VARCHAR }
        - { name: ccy,          type: VARCHAR }
        - { name: curve_id,     type: VARCHAR }
        - { name: tenor,        type: VARCHAR }
        - { name: pv01,         type: DOUBLE }
        - { name: booking_time, type: TIMESTAMP }

  - name: market_data
    mode: replace
    key_columns: [curve_id, ccy, tenor]
    schema:
      columns:
        - { name: curve_id,  type: VARCHAR }
        - { name: ccy,       type: VARCHAR }
        - { name: tenor,     type: VARCHAR }
        - { name: rate_bps,  type: DOUBLE }
        - { name: move_bps,  type: DOUBLE }

sources:
  - name: greeks_sod
    type: file
    table: greeks
    format: parquet
    load: reset
    path_resolver:
      function: get_sod_file
      inline: |
        local HOLIDAYS = { ["2026-01-01"]=true, ["2026-12-25"]=true }

        local function prev_business_day(iso)
          local t = os.time({
            year=tonumber(iso:sub(1,4)),
            month=tonumber(iso:sub(6,7)),
            day=tonumber(iso:sub(9,10))
          })
          repeat
            t = t - 86400
            local d = os.date("*t", t)
            local s = os.date("%Y-%m-%d", t)
          until d.wday ~= 1 and d.wday ~= 7 and not HOLIDAYS[s]
          return os.date("%Y%m%d", t)
        end

        function get_sod_file(ctx)
          return { "/data/greeks/" .. prev_business_day(ctx.today_iso) .. "_population.parquet" }
        end

  - name: greeks_intraday
    type: kafka
    brokers: ["localhost:9092"]
    topic: sensitivities.delta
    group_id: hydrocube-risk-greeks
    table: greeks
    format: json
    max_batch_size: 200
    batch_wait_ms: 50
    identity_key: trade_id
    transform:
      - type: lua
        function: handle_greeks
        inline: |
          -- Batch contract: receives up to max_batch_size messages.
          -- Explodes delta ladders and handles amendments via identity cache.
          function handle_greeks(batch, ctx)
            local to_lookup = {}
            for _, msg in ipairs(batch) do
              if ctx.seen("greeks", msg.trade_id) then
                to_lookup[#to_lookup + 1] = msg.trade_id
              end
            end

            local net = {}
            if #to_lookup > 0 then
              local ph = string.rep("?,", #to_lookup):sub(1, -2)
              local rows = ctx.query(
                "SELECT trade_id, curve_id, tenor, SUM(pv01) AS net " ..
                "FROM greeks WHERE trade_id IN (" .. ph .. ") " ..
                "GROUP BY trade_id, curve_id, tenor",
                table.unpack(to_lookup)
              )
              for _, r in ipairs(rows) do
                net[r.trade_id .. "|" .. r.curve_id .. "|" .. r.tenor] = r.net
              end
            end

            local out = {}
            for _, msg in ipairs(batch) do
              if ctx.seen("greeks", msg.trade_id) then
                for _, e in ipairs(msg.sensitivities or {}) do
                  local key = msg.trade_id .. "|" .. e.curve_id .. "|" .. e.tenor
                  local n = net[key]
                  if n and n ~= 0 then
                    out[#out + 1] = { trade_id=msg.trade_id, book=msg.book,
                      desk=msg.desk, ccy=msg.ccy, curve_id=e.curve_id,
                      tenor=e.tenor, pv01=-n, booking_time=msg.booking_time }
                  end
                end
              else
                ctx.mark_seen("greeks", msg.trade_id)
              end
              if msg.amendment_type ~= "CANCEL" then
                for _, e in ipairs(msg.sensitivities or {}) do
                  out[#out + 1] = { trade_id=msg.trade_id, book=msg.book,
                    desk=msg.desk, ccy=msg.ccy, curve_id=e.curve_id,
                    tenor=e.tenor, pv01=e.pv01, booking_time=msg.booking_time }
                end
              end
            end
            return out
          end

  - name: market_data_feed
    type: nats
    url: nats://localhost:4222
    subject: "rates.market_data.>"
    table: market_data
    format: json

window:
  interval_ms: 1000

persistence:
  enabled: true
  path: /data/hydrocube/rates_risk.db
  flush_interval: 10

retention:
  raw:
    duration: 5d
    parquet_path: /data/hydrocube/greeks_archive/
  aggregates:
    duration: forever

drillthrough:
  max_rows: 50000

aggregation:
  key_columns: [desk, book, ccy, curve_id, tenor]
  dimensions:  [desk, book, ccy, curve_id, tenor]
  measures:    [total_pv01, total_risk_bps]

  startup:
    sql: |
      -- The greeks_sod file source (load: reset) has already loaded Parquet rows
      -- into the greeks table before startup_sql runs. Read from greeks, not from
      -- parquet_scan directly — this keeps drill-through working and avoids
      -- double-counting if both paths were active.
      CREATE OR REPLACE TABLE sod_greeks AS
      SELECT desk, book, ccy, curve_id, tenor, SUM(pv01) AS total_pv01
      FROM greeks
      GROUP BY desk, book, ccy, curve_id, tenor;

      CREATE OR REPLACE TABLE intraday_greeks (
        desk VARCHAR, book VARCHAR, ccy VARCHAR,
        curve_id VARCHAR, tenor VARCHAR, total_pv01 DOUBLE
      );

  compaction:
    interval: 120s
    sql: |
      -- {pending.greeks} = greeks WHERE _window_id > {cutoff} AND _window_id <= {new_cutoff}
      -- After this runs, engine advances compaction_cutoff to {new_cutoff}.
      -- Raw rows are NOT deleted — they stay for drill-through.
      CREATE OR REPLACE TABLE intraday_greeks AS
      SELECT desk, book, ccy, curve_id, tenor,
        SUM(total_pv01) AS total_pv01
      FROM (
        SELECT desk, book, ccy, curve_id, tenor, total_pv01 FROM intraday_greeks
        UNION ALL
        SELECT desk, book, ccy, curve_id, tenor, SUM(pv01) AS total_pv01
        FROM {pending.greeks}
        GROUP BY desk, book, ccy, curve_id, tenor
      ) t
      GROUP BY desk, book, ccy, curve_id, tenor;

  publish:
    sql: |
      -- {greeks} = greeks WHERE _window_id > {compaction_cutoff}
      -- Three tiers ensure every greek is counted exactly once.
      -- Market data join re-evaluates on every tick without touching greek aggregates.
      SELECT
        g.desk, g.book, g.ccy, g.curve_id, g.tenor,
        SUM(g.total_pv01)              AS total_pv01,
        SUM(g.total_pv01 * m.move_bps) AS total_risk_bps
      FROM (
        SELECT desk, book, ccy, curve_id, tenor, total_pv01 FROM sod_greeks
        UNION ALL
        SELECT desk, book, ccy, curve_id, tenor, total_pv01 FROM intraday_greeks
        UNION ALL
        SELECT desk, book, ccy, curve_id, tenor, SUM(pv01) AS total_pv01
        FROM {greeks}
        GROUP BY desk, book, ccy, curve_id, tenor
      ) g
      JOIN market_data m
        ON  g.curve_id = m.curve_id
        AND g.ccy      = m.ccy
        AND g.tenor    = m.tenor
      GROUP BY g.desk, g.book, g.ccy, g.curve_id, g.tenor

  snapshots:
    - name: eod_risk
      schedule: "0 17 * * 1-5"
      sql: |
        CREATE TABLE IF NOT EXISTS eod_risk_snapshots (
          snap_time      TIMESTAMP,
          desk           VARCHAR,
          book           VARCHAR,
          ccy            VARCHAR,
          curve_id       VARCHAR,
          tenor          VARCHAR,
          total_pv01     DOUBLE,
          total_risk_bps DOUBLE
        );

        INSERT INTO eod_risk_snapshots
        SELECT CURRENT_TIMESTAMP AS snap_time, desk, book, ccy, curve_id, tenor,
          total_pv01, total_risk_bps
        FROM ({publish_sql}) s;

  housekeeping:
    - name: trim_eod_snapshots
      schedule: "0 2 * * 1-5"      # 02:00 weekdays — after trading day is safely closed
      sql: |
        DELETE FROM eod_risk_snapshots
        WHERE snap_time < NOW() - INTERVAL 90 DAYS;

  reset:
    schedule: "0 7 * * 1-5"
    sql: |
      DROP TABLE IF EXISTS sod_greeks;
      DROP TABLE IF EXISTS intraday_greeks;

  reaggregation:
    schedule: "0 6 * * 1-5"
```

---

### Pattern 3 — MtM P&L

**Use case:** Current positions (replace) joined to live prices (replace). Real-time P&L = `SUM(position_size × (mid_price − cost_price))` by book.

```yaml
name: mtm_pnl

tables:
  - name: positions
    mode: replace
    key_columns: [book, instrument_id]
    schema:
      columns:
        - { name: book,          type: VARCHAR }
        - { name: instrument_id, type: VARCHAR }
        - { name: position_size, type: DOUBLE }
        - { name: cost_price,    type: DOUBLE }

  - name: prices
    mode: replace
    key_columns: [instrument_id]
    schema:
      columns:
        - { name: instrument_id, type: VARCHAR }
        - { name: mid_price,     type: DOUBLE }

sources:
  - name: positions_feed
    type: kafka
    brokers: ["localhost:9092"]
    topic: positions.live
    group_id: hydrocube-pnl-positions
    table: positions
    format: json

  - name: prices_feed
    type: kafka
    brokers: ["localhost:9092"]
    topic: prices.mid
    group_id: hydrocube-pnl-prices
    table: prices
    format: json

window:
  interval_ms: 1000

aggregation:
  key_columns: [book]
  dimensions:  [book]
  measures:    [total_pnl]

  publish:
    sql: |
      SELECT p.book,
        SUM(p.position_size * (pr.mid_price - p.cost_price)) AS total_pnl
      FROM positions p
      JOIN prices pr ON p.instrument_id = pr.instrument_id
      GROUP BY p.book

delta:
  epsilon: 0.01    # suppress updates where P&L moves less than 1 cent
```

**Key insight:** Both tables are replace mode. No tiers, no compaction, no startup SQL. Publish cost is `O(positions × prices join)`, constant for the life of the engine.

---

### Pattern 4 — YTD retail sales

**Use case:** Sales events all year. Users want `SUM(revenue)` by product, region, and month. Resets on 1 January.

```yaml
name: ytd_sales

tables:
  - name: sales
    mode: append
    schema:
      columns:
        - { name: product,   type: VARCHAR }
        - { name: region,    type: VARCHAR }
        - { name: month,     type: VARCHAR }
        - { name: revenue,   type: DOUBLE }

sources:
  - name: sales_stream
    type: kafka
    brokers: ["localhost:9092"]
    topic: sales.events
    group_id: hydrocube-ytd-sales
    table: sales
    format: json

window:
  interval_ms: 1000

retention:
  raw:
    duration: 400d    # retain full year + buffer for drill-through
  aggregates:
    duration: forever

aggregation:
  key_columns: [product, region, month]
  dimensions:  [product, region, month]
  measures:    [total_revenue, transaction_count]

  startup:
    sql: |
      CREATE OR REPLACE TABLE ytd_aggregate AS
      SELECT product, region, month,
        SUM(revenue) AS total_revenue,
        COUNT(*)     AS transaction_count
      FROM sales
      GROUP BY product, region, month;

      CREATE OR REPLACE TABLE intraday_sales
        AS SELECT * FROM ytd_aggregate WHERE false;

  compaction:
    interval: 300s
    sql: |
      CREATE OR REPLACE TABLE intraday_sales AS
      SELECT product, region, month,
        SUM(total_revenue)     AS total_revenue,
        SUM(transaction_count) AS transaction_count
      FROM (
        SELECT * FROM intraday_sales
        UNION ALL
        SELECT product, region, month,
          SUM(revenue) AS total_revenue, COUNT(*) AS transaction_count
        FROM {pending.sales}
        GROUP BY product, region, month
      ) t
      GROUP BY product, region, month;

  publish:
    sql: |
      SELECT product, region, month,
        SUM(total_revenue)     AS total_revenue,
        SUM(transaction_count) AS transaction_count
      FROM (
        SELECT * FROM ytd_aggregate
        UNION ALL
        SELECT * FROM intraday_sales
        UNION ALL
        SELECT product, region, month,
          SUM(revenue) AS total_revenue, COUNT(*) AS transaction_count
        FROM {sales}
        GROUP BY product, region, month
      ) t
      GROUP BY product, region, month

  reset:
    schedule: "0 0 1 1 *"
    sql: |
      DROP TABLE IF EXISTS ytd_aggregate;
      DROP TABLE IF EXISTS intraday_sales;
```

---

### Pattern 5 — IoT rolling window

**Use case:** High-frequency sensor readings. Users want `AVG(temperature)`, `MAX(pressure)`, `MIN(voltage)` over the last 4 hours by sensor and location. Readings older than 4 hours are irrelevant.

```yaml
name: sensor_rolling

tables:
  - name: readings
    mode: append
    event_time_column: event_time
    schema:
      columns:
        - { name: sensor_id,   type: VARCHAR }
        - { name: location,    type: VARCHAR }
        - { name: temperature, type: DOUBLE }
        - { name: pressure,    type: DOUBLE }
        - { name: voltage,     type: DOUBLE }
        - { name: event_time,  type: TIMESTAMP }

sources:
  - name: sensor_feed
    type: kafka
    brokers: ["localhost:9092"]
    topic: sensors.readings
    group_id: hydrocube-sensors
    table: readings
    format: json
    max_batch_size: 500
    batch_wait_ms: 100

window:
  interval_ms: 1000

retention:
  raw:
    duration: 4h

aggregation:
  key_columns: [sensor_id, location]
  dimensions:  [sensor_id, location]
  measures:    [avg_temperature, max_pressure, min_voltage]

  publish:
    sql: |
      SELECT sensor_id, location,
        AVG(temperature) AS avg_temperature,
        MAX(pressure)    AS max_pressure,
        MIN(voltage)     AS min_voltage
      FROM readings
      WHERE event_time >= NOW() - INTERVAL 4 HOURS
      GROUP BY sensor_id, location
```

**Key insight:** No startup, no compaction, no tiers. Single `publish_sql` over the raw table. Retention policy keeps the table bounded. Sensors that stop sending readings disappear from the result — DELETE signals are emitted to Perspective automatically by the delta detection mechanism.

---

## Pattern Summary

| Pattern | Tiers | Hooks used | Compaction | Join |
|---|---|---|---|---|
| 1 — Day-population cumulative | 3 | startup, compaction, publish, reset | Rebuild intraday | No |
| 2 — Real-time risk | 3 (greeks) + replace (market data) | startup, compaction, publish, snapshot, reset | Rebuild intraday | Yes |
| 3 — MtM P&L | 0 (two replace tables) | publish only | No | Yes |
| 4 — YTD sales | 3 | startup, compaction, publish, reset | Rebuild intraday | No |
| 5 — IoT rolling | 0 (raw only) | publish only | No (age retention) | No |

---

## Test Matrix

### Compaction correctness

| Scenario | Assertion |
|---|---|
| 1M bootstrap rows compacted into sod_aggregate, 10k intraday rows arrive | Publish total = SUM of all 1,010,000 rows |
| Compaction runs while messages are arriving | No row counted twice, no row missed — {pending} boundary is exclusive |
| Day reset fires | Aggregates dropped, next publish emits INSERT for all new-day rows, no prior-day values |
| YTD: simulate 365 days of daily compaction cycles | Final total = SUM of all year's raw events |
| Year-end reset | Post-reset shows zero totals; raw rows from prior year present and queryable via drill-through |

### Delta detection correctness

| Scenario | Assertion |
|---|---|
| New trade arrives in book=EMEA | Only EMEA row in delta — all others suppressed |
| Market data tick changes all tenors | All risk rows in delta |
| Sensor stops sending | Key disappears from result — DELETE signal emitted |
| Day reset | All prior-day keys receive DELETE before new-day INSERTs |
| P&L crosses zero | UPDATE signal emitted with correct signed value |
| P&L changes by less than epsilon | No signal emitted |

### Amendment and identity cache correctness

| Scenario | Assertion |
|---|---|
| New trade processed | Trade ID added to identity cache, fast path taken |
| Same trade re-sent (Kafka redelivery) | Slow path taken, reversal + re-emit produced, net aggregate change = 0 |
| Trade amendment arrives | Slow path taken, synthetic reversal generated, corrected values land, aggregate updated correctly |
| Multiple amendments in one batch | Single batched DuckDB lookup, all handled correctly |
| Engine restart | Identity cache pre-populated from raw table, no known trade misclassified as new |
| Trade cancelled, same ID reused | Slow path taken, net lookup returns 0, no reversal emitted, new trade lands correctly |

### Drill-through correctness

| Scenario | Assertion |
|---|---|
| Drill-through on book=EMEA, Pattern 1 | All raw trade rows with book=EMEA returned, regardless of compaction cutoff |
| Drill-through exceeds 50,000 rows | HTTP 413 with clear message |
| Drill-through on replace table | HTTP 400 with clear message |
| Drill-through after day reset | Prior-day raw rows present (within retention window) |

### Re-aggregation correctness

| Scenario | Assertion |
|---|---|
| Correct trade row injected, POST /api/reaggregate triggered | Next publish reflects corrected aggregate |
| Re-aggregation during live ingest | No trade lost — startup_sql reads current MAX(_window_id), subsequent messages land in gap |
| Pattern 3 (no startup_sql) receives POST /api/reaggregate | 200 OK, no-op |

### Startup and reset edge cases

| Scenario | Assertion |
|---|---|
| Empty append table at startup | startup_sql produces empty aggregate, normal operation proceeds |
| SOD file not found | startup_sql fails, engine refuses to start, clear error logged |
| Engine restart mid-day | compaction_cutoff restored from persistence, {trades} contains only post-cutoff rows |
| Reset SQL fails | Previous period aggregates remain, engine continues, failure surfaced at /api/status |
| Two reset sources declared with load: reset | Both loaded in declared order before reset SQL runs |

### File source and path resolution

| Scenario | Assertion |
|---|---|
| Path resolver called at startup | Correct file for today's date loaded |
| Path resolver called at reset | Correct file for new day loaded (not startup day) |
| Path resolver returns multiple files | All files loaded in order, all rows inserted into table |
| Path resolver raises Lua error | Engine refuses to start with clear error |
| Parquet scan via {path.source_name} | Token expands to correct path, DuckDB reads file directly |

### Multi-table join correctness

| Scenario | Assertion |
|---|---|
| Greek arrives with no matching market_data row | Row excluded from result (inner JOIN semantics) — documented behaviour |
| Market data tenor removed mid-session | Risk rows for that tenor deleted from result, DELETE signal emitted |
| Position with no matching price | Row absent from P&L — consistent with JOIN semantics |

---

## Operational Notes

**Perspective row limit:** publish SQL should produce no more than ~10,000 rows for smooth live updating. Beyond this, Arrow IPC transfer every second becomes the bottleneck. A warning is logged but publishing continues.

**Drill-through row limit:** default 50,000 rows. Configurable. Users who select very few dimensions will get large result sets — this is by design and documented as their responsibility.

**Append table amendment pattern:** `COUNT(*)` aggregates are incorrect when reversals are in use. Use `SUM(trade_count_contribution)` where new bookings carry `+1` and cancellations carry `−1`.

**Bucketing:** time and numeric bucketing are expressed in SQL (DuckDB's `DATE_TRUNC`, `TIME_BUCKET`, `WIDTH_BUCKET`, `FLOOR`) or pre-computed at ingest time via Lua. No built-in bucketing config — the cookbook documents standard patterns.

**Snapshot tables grow unboundedly** unless the user includes a DELETE at the top of snapshot SQL. Standard pattern documented in the cookbook.

**batch_wait_ms** should not exceed `window.interval_ms / 2`. Breaching this causes ingest to lag behind the publish window.

**Re-aggregation correctness window:** re-aggregation is only correct within the raw retention window. Raw rows older than `retention.raw.duration` may have been deleted. Full historical re-aggregation requires reading from the Parquet archive in `startup_sql`.

# Time Model Design

**Date:** 2026-04-21
**Status:** Approved for implementation planning

---

## Problem

HydroCube currently assigns `_window_id` based on message arrival time (processing time). This is simple but incorrect for sources that carry their own timestamps — late-arriving messages land in the wrong window, and there is no way to express time-based aggregations that look back further than one window interval. Real deployments need event time bucketing, configurable late data handling, sliding windows for trend analytics, and calendar-aligned snapshots for period-end reporting.

---

## Decisions

### Event time is opt-in per table

Processing time bucketing remains the default. Event time is enabled by declaring `event_time_column` on a table. This avoids breaking existing deployments and keeps the fast path simple for sources where arrival time is a good-enough proxy for event time (most market data, IoT sensor feeds).

### Late data policy: current_window default, drop opt-in

When a message arrives with an event timestamp behind the current window boundary, the default is to land it in the current window (`current_window`). This preserves the data — the event timestamp column still carries the correct time so downstream queries work accurately — at the cost of slight window mis-attribution for aggregated counts and sums. For most use cases (sums, averages, latest values) this is acceptable.

`drop` is available as a per-source opt-in for high-frequency replace/LVC feeds where a late tick is meaningless (the next one will correct it anyway).

Option B (reopen and re-flush closed windows with corrective deltas) is explicitly deferred from v1: it is complex to implement correctly and disorienting for users who see settled values change retroactively.

### Sliding windows via DuckDB window functions

Rather than managing rolling buffers in the engine, sliding windows are expressed in the aggregation SQL using DuckDB's native time-range filtering and window functions. The engine's role is limited to: keeping enough history in DuckDB to cover the declared `size` period, and flushing on the declared `interval_ms`. This keeps the engine simple and gives users full SQL expressiveness for the lookback calculation.

### Calendar windows as end-of-period snapshots

Calendar windows fire alongside a regular tumbling window — the tumbling window drives the live view at `interval_ms`; the calendar boundary triggers an additional flush that writes a named snapshot to a `period_snapshots` table. This avoids replacing the live view mechanism while giving users a clean accumulation of period-end state.

---

## Config model

### Event time declaration

Declared per table. Replace and reference tables ignore it.

```yaml
tables:
  - name: slices
    mode: append
    event_time_column: trade_time    # use this column for windowing
    schema:
      columns:
        - name: trade_time
          type: TIMESTAMP
        - name: notional
          type: DOUBLE
```

When `event_time_column` is declared, the engine reads the timestamp from each arriving message and uses it to assign `_window_id`. If the column is absent or null in a specific message, the engine falls back to processing time for that message only.

### Late data policy

Declared per source. Default is `current_window`.

```yaml
sources:
  - type: kafka
    topic: trades.executed
    table: slices
    late_data: current_window   # default — land in current window, no data loss

  - type: nats
    subject: rates.live
    table: market_data
    late_data: drop             # high-frequency LVC feed — late ticks are meaningless
```

### Window types

Declared in the `window:` block. Mutually exclusive — one type per cube.

```yaml
# Tumbling (default) — existing behaviour unchanged
window:
  type: tumbling
  interval_ms: 1000

# Sliding — rolling lookback, re-evaluated every interval_ms
window:
  type: sliding
  size: 5m
  interval_ms: 1000

# Calendar — period-end snapshots alongside tumbling live view
window:
  type: calendar
  period: daily          # daily | weekly | monthly
  timezone: Europe/London
  interval_ms: 1000      # tumbling interval for live updates between boundaries
  close_time: "17:30"    # optional: fire at 17:30 rather than midnight (UTC if timezone not set)
```

---

## Window types in detail

### Tumbling

Existing behaviour, unchanged. Fixed non-overlapping intervals driven by `interval_ms`. `_window_id` increments by 1 on each flush. Compaction and retention operate on `_window_id` as today.

### Sliding

A window of fixed `size` that re-evaluates every `interval_ms`. The engine ensures data within the `size` period is protected from compaction — the effective compaction cutoff is `max(retention_cutoff, now - size)`. Data older than `size` follows the normal retention policy.

The aggregation SQL uses the event time column and DuckDB's time arithmetic to define the lookback:

```sql
SELECT
  book,
  AVG(price)    AS avg_price_5m,
  SUM(notional) AS rolling_notional_5m
FROM slices
WHERE trade_time >= NOW() - INTERVAL 5 MINUTES
GROUP BY book
```

`event_time_column` must be declared on append tables used in sliding window aggregations. The engine validates this at startup and exits with a clear error if it is missing.

### Calendar

Fires at calendar boundaries (midnight, week start, month start) in the declared `timezone`, or at `close_time` if specified. Between boundaries, a tumbling window at `interval_ms` drives the live Perspective view as normal.

When a calendar boundary is crossed the engine:
1. Performs a full aggregation flush (identical to a tumbling flush)
2. Writes the result to the `period_snapshots` table:

```sql
CREATE TABLE period_snapshots (
    period_start  TIMESTAMP,
    period_end    TIMESTAMP,
    period_label  VARCHAR,      -- e.g. "2026-04-21" for daily
    -- all columns from the aggregation SQL result
    ...
);
```

`period_snapshots` accumulates over time. It is queryable via the drill-down API and freely joinable in aggregation SQL. It is archived to Parquet as part of the normal retention cycle. It is never compacted — each period's snapshot is a permanent record.

Example aggregation SQL that surfaces period-end comparison alongside the live view:

```sql
SELECT
  s.book,
  SUM(s.notional)                       AS live_notional,
  snap.total_notional                   AS sod_notional,
  SUM(s.notional) - snap.total_notional AS intraday_change
FROM slices s
LEFT JOIN period_snapshots snap
  ON snap.period_label = CURRENT_DATE::VARCHAR
  AND snap.book = s.book
GROUP BY s.book, snap.total_notional
```

---

## Engine time model: complete sequence

```
Message arrives from source
        ↓
event_time_column declared on target table?
    Yes → read timestamp from message field
          compare to current window boundary
          within boundary  → assign to current window
          behind boundary  → apply source late_data policy:
                               current_window: assign to current window
                               drop: discard, increment late_dropped counter
    No  → assign to current window (processing time — current behaviour)
        ↓
Message enters per-table buffer (append Vec or replace HashMap)
        ↓
Window flush trigger:
    tumbling  → every interval_ms
    sliding   → every interval_ms
    calendar  → every interval_ms + additionally at each boundary crossing
        ↓
Aggregation SQL runs over available history
        ↓
calendar boundary? → write result to period_snapshots
        ↓
Delta detect on aggregation result → broadcast via SSE
        ↓
Commit source offsets / ack JetStream messages
```

---

## Observability

Two new counters exposed on `GET /api/status` per table:

| Counter | Meaning |
|---|---|
| `late_dropped` | Messages discarded by `late_data: drop` policy |
| `late_landed` | Messages with past event timestamps landed in current window |

Both reset on process restart.

---

## Interaction with window type and event time

| Window type | Event time column | Behaviour |
|---|---|---|
| Tumbling | Not declared | Processing time bucketing — current behaviour |
| Tumbling | Declared | Event time bucketing with late data policy applied |
| Sliding | Must be declared | Lookback calculated from event timestamps in SQL |
| Calendar | Optional | Boundary detection uses processing time; event time used in SQL filters if declared |

---

## What this deliberately does not include

- **Session windows** — windows bounded by inactivity gaps. Useful for clickstream and user journey analysis but complex to implement; deferred.
- **Watermarks** — formal mechanism for declaring when a window is definitively closed. The `late_data: drop` policy is a simplified equivalent covering the majority of cases.
- **Reopening closed windows** — retroactive corrections when late data arrives after a window is closed. Deferred: complex to implement and disorienting in the live UI.
- **Multiple window types per cube** — one type per cube keeps the engine straightforward. Cross-window analysis is done in aggregation SQL by querying `period_snapshots` alongside live `slices`.
- **Business calendars with holidays** — calendar windows use standard ISO calendar (daily/weekly/monthly) only. Custom holiday calendars are not supported in v1.

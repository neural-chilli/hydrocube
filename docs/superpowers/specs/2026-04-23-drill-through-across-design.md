# Drill-Through and Drill-Across Design

**Date:** 2026-04-23
**Status:** Approved for implementation planning

---

## Problem

HydroCube currently presents a single aggregated live view per cube. Users have no way to interrogate what is behind an aggregated value (drill-through) or to navigate to a related cube filtered to the same dimensional context (drill-across). Both capabilities are essential for operational analytics where insight happens at the point of investigation, not at the point of report design.

---

## Concepts

### Drill-through

Vertical movement. Within a single cube, breaking through the aggregation surface to see the raw source rows that contributed to a specific aggregated value. Analogous to double-clicking a cell in an Excel pivot table and seeing a new sheet of underlying records.

### Drill-across

Horizontal movement. Navigating from one cube to a related cube, carrying shared dimension values as filters. The user lands in a live view of the target cube scoped to the same dimensional context they were examining. Insight happens in the traversal — following a thread from one live window of reality to another.

### What this is not

Drill-down (navigating a dimension hierarchy within the same view) is handled natively by Perspective's row pivot capability and requires no engine support.

---

## Design principles

- **No inference.** Relationships between cubes must be declared explicitly in config. Data that informs multi-million pound decisions must have deliberate, auditable relationships.
- **No cross-engine queries.** Cube engines never query each other. All drill-across intelligence lives in the client. Engines publish metadata; clients navigate using it.
- **Multi-hop falls out naturally.** Relationships are declared independently per cube. If A relates to B and B relates to C, traversing A → B → C requires no additional engineering — each cube's relationships are available from its peer registration regardless of how the user arrived there.
- **Explicit bidirectionality.** If A declares a relationship to B, B must also declare a relationship to A for the traversal to work in both directions. This costs a little more in config but keeps relationships deliberate and each cube's owner in control of what they expose.

---

## Drill-through

### Engine endpoint

```
GET /api/drillthrough/{table_name}?{dimension_filters}&_window_id={n}
```

Returns the raw source rows from `{table_name}` matching the provided dimension filters, scoped to the specified window. If `_window_id` is omitted, defaults to the current window.

**Example:**

The user right-clicks a row where `book = 'EMEA'` and `asset_class = 'Rates'` in the trades cube. The client calls:

```
GET /api/drillthrough/slices?book=EMEA&asset_class=Rates&_window_id=42
```

The engine executes:

```sql
SELECT * FROM slices
WHERE book = 'EMEA'
  AND asset_class = 'Rates'
  AND _window_id = 42
```

Response format: Arrow IPC (same as `/api/snapshot`), with row count and base64 payload.

**Parameter construction:** the client derives filter parameters from the GROUP BY columns of the aggregation SQL. The values come from the right-clicked row in the Perspective grid via `view.to_json()`.

**Auth:** same token enforcement as all other endpoints.

**Replace and reference tables:** drill-through is only meaningful for append tables. Replace tables carry only the latest value per key; reference tables are static. The endpoint returns `400` with a clear error if called against a non-append table.

### Result display

Drill-through results open in a new Perspective grid rendered in a modal overlay. The modal title shows the dimension values used as filters (e.g. "EMEA · Rates · Window 42"). The grid is static — it shows the rows as they were at that window, not a live updating view.

---

## Drill-across

### Config

Dimensions and relationships are declared on the `cube:` block in `cube.yaml`.

```yaml
cube:
  name: trades
  description: "Trade execution feed"
  dimensions:
    - book
    - trader
    - asset_class
    - currency
  relationships:
    - cube: market-risk
      url: http://market-risk-host:8080
      via: [book, asset_class]
      label: "View market risk"
    - cube: counterparty-exposure
      url: http://counterparty-host:8080
      via: [book]
      label: "View counterparty exposure"
```

```yaml
cube:
  name: market-risk
  description: "Live market risk by book"
  dimensions:
    - book
    - asset_class
    - risk_type
  relationships:
    - cube: trades
      url: http://trades-host:8080
      via: [book, asset_class]
      label: "View trades"
    - cube: counterparty-exposure
      url: http://counterparty-host:8080
      via: [book]
      label: "View counterparty exposure"
```

**Bidirectionality is explicit.** Each cube declares its own outbound relationships. A relationship from trades to market-risk does not automatically create a reverse relationship — market-risk must declare its own relationship back to trades.

### Peer registry extension

When a cube registers with its seed peers, the registration payload is extended to include dimensions and relationships:

```json
{
  "name": "trades",
  "url": "http://trades-host:8080",
  "description": "Trade execution feed",
  "status": "healthy",
  "dimensions": ["book", "trader", "asset_class", "currency"],
  "relationships": [
    {
      "cube": "market-risk",
      "url": "http://market-risk-host:8080",
      "via": ["book", "asset_class"],
      "label": "View market risk"
    }
  ]
}
```

`GET /api/peers` returns this full metadata for every registered cube. The client fetches this once on load and uses it to build the relationship graph.

### Context carryover

When the user drills across from Cube A to Cube B, all dimension values that are declared in the relationship's `via` list are carried as filters to the target cube. The client:

1. Reads the current row context from Perspective (`view.to_json()` at the clicked row)
2. Extracts the values for the `via` dimensions (e.g. `book = 'EMEA'`, `asset_class = 'Rates'`)
3. Opens a WebSocket connection to the target cube's URL
4. Constructs a Perspective viewer config for the target cube with those values applied as filters
5. Renders a live grid — not a snapshot, a live updating view of the target cube in that dimensional context

The user arrives in the target cube's live view, scoped as tightly as the shared dimensions allow.

Dimensions present in the current Perspective view that are not in the `via` list are not carried — only declared shared dimensions cross the boundary.

### Navigation model

On triggering a drill-across, the client presents a one-time prompt:

> Open "View market risk" in:
> ○ This window
> ○ New window
> ☐ Always use this choice (reset in user menu)

The preference is stored in browser localStorage and respected until the user resets it via the user menu. No server-side persistence required.

### Cube unavailability

When building the right-click context menu, the client checks the `status` field of each related cube from the peer registry data. Cubes with status other than `healthy` are shown in the menu but greyed out with a tooltip: "Unavailable".

The peer registry data is refreshed periodically (same interval as the existing health check gossip) so availability state stays current without requiring a page reload.

---

## Right-click context menu

Both drill-through and drill-across are accessed via a custom context menu, triggered by right-clicking any row in the Perspective grid.

The menu is contextual — it shows only options relevant to the clicked row:

```
─────────────────────────────
  Book A · EMEA · Rates
─────────────────────────────
  Drill through              →  raw rows for this value
─────────────────────────────
  View market risk           →  (live, available)
  View counterparty exposure →  (greyed out — unavailable)
─────────────────────────────
```

Labels come from the `label:` field in the relationship config. The section header shows the dimension values in the clicked row to confirm context.

**Implementation:** a thin JavaScript overlay on the Perspective viewer container intercepts the `contextmenu` event, suppresses the browser default, and renders the custom menu. Perspective's own context menu (if any) is replaced entirely.

---

## The semantic layer

What emerges from dimensions and relationships declared across a network of cubes is a lightweight semantic layer: a graph where each node is a live analytics view and each edge is a traversable relationship anchored in shared business dimensions.

Multi-hop traversal — trades → market risk → counterparty exposure → counterparty master — requires no additional engineering. Each cube's relationships are declared independently and available from its peer registration regardless of how the user arrived at the current view. The graph is navigable from any node.

This transforms HydroCube from a single real-time window into a navigable network of interconnected live views. Insight happens at the traversal points — following a thread from an unexpected value in one domain to its cause or consequence in another, in real time.

---

## Engine changes summary

| Component | Change |
|---|---|
| `cube.yaml` | New `dimensions` and `relationships` fields on `cube:` block |
| `src/config.rs` | New `CubeDimensions`, `CubeRelationship` config structs |
| `src/web/api.rs` | New `GET /api/drillthrough/{table}` handler |
| Peer registry | Registration payload extended with dimensions + relationships |
| `GET /api/peers` | Returns full metadata including dimensions and relationships |
| Web UI (JS) | Right-click context menu, drill-through modal, drill-across navigation, preference storage |

---

## What this deliberately does not include

- **Inferred relationships** — all relationships are declared explicitly. No automatic discovery of joinable dimensions.
- **Cross-engine SQL joins** — engines never query each other. All cross-cube assembly happens in the client.
- **Dimension hierarchies** — navigating within a dimension (continent → country → city) is handled by Perspective's native row pivot capability, not by HydroCube.
- **Relationship cardinality validation** — HydroCube does not validate that declared `via` dimensions exist in the target cube's aggregation SQL result. This is the cube owner's responsibility.
- **Drill-across history** — the target cube opens in its current live state. Historical drill-across (jump to what market risk looked like at window 42) is deferred.

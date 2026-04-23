# Output Layer Design

**Date:** 2026-04-23
**Status:** Approved for implementation planning

---

## Problem

HydroCube currently delivers aggregated window results via SSE only. Real deployments need to reach clients beyond the browser — native applications, operational monitoring tools, enterprise messaging infrastructure, and alert systems. The output layer must be extended without compromising the simplicity of the core engine or conflating HydroCube's role with that of a general-purpose stream processor.

---

## Positioning

HydroCube is not a stream processor. It is an end-to-end solution for the specific pattern: ingest → aggregate → live analytics. The output layer reflects this:

- **Tier 1 — Live UI outputs** are the core product. They deliver delta frames to clients displaying live grids.
- **Tier 2 — Integration outputs** allow HydroCube to act as a composable stage in a larger architecture — publishing clean aggregated results to systems that need them, without those systems having to do the aggregation themselves.

The two tiers have different contracts. Tier 1 delivers deltas at-most-once in real time. Tier 2 delivers complete window snapshots, best-effort, to downstream consumers.

---

## Tier 1 — Live UI Outputs

### SSE (existing)

Unchanged. Delta Arrow IPC frames, base64-encoded, delivered as `text/event-stream`. Remains the primary browser output mechanism.

### WebSocket (new)

**Endpoint:** `GET /ws` — upgrades to WebSocket using Axum's built-in `WebSocketUpgrade`.

**Server → client:** binary frames carrying the same base64 Arrow IPC payload as SSE, sourced from the same `tokio::broadcast` channel. One subscriber per connected client.

**Client → server:** a single supported message type:

```json
{ "type": "snapshot" }
```

On receipt the server runs the aggregation query immediately and sends the full result as a binary frame. All other client messages are ignored.

**Auth:** same token check as SSE, enforced at the upgrade handshake before the connection is accepted.

**Backpressure:** if a slow client's per-connection send buffer fills, the server closes that connection and logs a warning. Equivalent behaviour to SSE lagging.

**Why WebSocket over SSE for some clients:** WebSocket is a persistent bidirectional connection. It works in environments where HTTP streaming is restricted (native apps, desktop widgets, mobile clients, headless services) and enables client-initiated snapshot requests without a separate HTTP call.

---

## Tier 2 — Integration Outputs

All Tier 2 outputs are optional. Each is enabled by declaring the relevant block under `publish:` in `cube.yaml`. When the block is absent, no publisher is created.

All Tier 2 publishers:
- Run delivery asynchronously — they never block the flush cycle
- Increment `publish_errors` on `/api/status` on failure
- Log failures at `warn` level and continue

### Webhooks

HTTP POST to one or more configured URLs. Threshold-triggered or on every flush.

```yaml
publish:
  webhooks:
    - url: https://hooks.slack.com/services/xxx
      on: threshold
      condition: "total_exposure > 1000000"
      throttle_ms: 60000
      payload: json

    - url: https://internal.example.com/hydrocube/alert
      on: every_flush
      payload: json
```

**Trigger modes:**

| Mode | Behaviour |
|---|---|
| `threshold` | Evaluates `condition` (SQL expression) against aggregation result after each flush; fires if true |
| `every_flush` | Fires unconditionally on every window flush |

**Payload:** JSON object containing aggregation result rows, window timestamp, and (for threshold hooks) the condition that triggered the delivery.

**Delivery:** fire-and-forget. The flush cycle hands the payload to a per-webhook async delivery task and immediately continues. Webhook HTTP responses are not awaited by the flush cycle.

**Per-webhook delivery queue:** capped at 100 pending deliveries by default (configurable). Overflow drops with a `warn` log and increments `publish_errors`. Prevents unbounded memory growth if a webhook endpoint is slow or unavailable.

**Throttling:** `throttle_ms` prevents alert storms when a threshold condition persists across many windows. A webhook will not fire more than once per `throttle_ms` regardless of how many windows trigger the condition.

**Outbound auth:** optional bearer token or basic auth per webhook:

```yaml
    - url: https://internal.example.com/hydrocube/alert
      on: every_flush
      payload: json
      auth:
        type: bearer
        token: "${WEBHOOK_TOKEN}"
```

### MQTT

Publishes the full aggregation result to an MQTT topic after each window flush.

```yaml
publish:
  mqtt:
    broker: mqtt://localhost:1883
    topic_prefix: hydrocube/myapp
    format: json    # json only for v1
    qos: 0          # at-most-once — matches Tier 2 delivery contract
```

Results are published to `{topic_prefix}/output`. The payload is the full aggregation result as a JSON object, identical in structure to the `/api/snapshot` response.

QoS 0 (at-most-once) is the only supported level in v1. This matches the broader Tier 2 delivery contract — downstream consumers that need guaranteed delivery should use the snapshot API to recover missed windows.

### NATS

Publishes the full aggregation result to a NATS subject after each window flush. Completes the partially-built `NatsPublisher` in `src/publish.rs`.

```yaml
publish:
  nats:
    url: nats://localhost:4222
    subject_prefix: hydrocube.myapp
    format: json    # json | arrow
```

Results are published to `{subject_prefix}.output`. At-most-once (Core NATS). Format `arrow` sends raw Arrow IPC bytes for performance-sensitive consumers.

A snapshot can be requested by publishing to `{subject_prefix}.snapshot.request` — the engine publishes the current aggregation result to `{subject_prefix}.snapshot` in response.

**Implementation note:** `NatsPublisher::publish()` and `NatsPublisher::connect()` are already implemented. Wiring requires: instantiating the publisher in `main.rs`, passing it to the engine task, and calling `publish()` after each flush alongside the SSE broadcast.

### Kafka

Publishes the full aggregation result to a Kafka topic after each window flush.

```yaml
publish:
  kafka:
    brokers: ["localhost:9092"]
    topic: hydrocube.myapp.output
    format: json    # json | arrow
```

Payload is the full aggregation result (not a delta). Downstream consumers treat each message as a complete replacement of the previous window state.

Delivery is best-effort. Publish failures log at `warn` and increment `publish_errors` but do not roll back the window or affect ingest offset commits.

**Priority note:** Kafka publish is the lowest-priority Tier 2 output. The Kafka dependency already exists for ingest, making this straightforward to add, but the use case is narrow — teams with existing Kafka infrastructure that want HydroCube to feed a native Kafka consumer.

---

## Engine changes

### New config shape

```yaml
publish:
  webhooks: [...]     # optional list
  mqtt: {...}         # optional block
  nats: {...}         # optional block
  kafka: {...}        # optional block
```

### AppState additions

```rust
struct AppState {
    // existing fields ...
    webhook_publishers: Vec<WebhookPublisher>,
    mqtt_publisher:     Option<MqttPublisher>,
    nats_publisher:     Option<NatsPublisher>,   // already exists
    kafka_publisher:    Option<KafkaPublisher>,
}
```

### Flush sequence addition

After delta detect and SSE broadcast:

```
1. Broadcast delta via SSE
2. Broadcast delta via WebSocket (same broadcast channel)
3. For each Tier 2 publisher present:
       serialise full aggregation result once per format (json / arrow)
       hand serialised payload to publisher's async delivery task
4. Commit ingest offsets
```

Serialisation happens once per format regardless of how many publishers share that format.

### New status counters

| Counter | Meaning |
|---|---|
| `publish_errors` | Failed Tier 2 delivery attempts (per publisher) |
| `webhook_queue_depth` | Current pending deliveries per webhook (gauge) |

---

## Roadmap (out of scope for this spec)

### Excel Office Add-in

A TypeScript Office.js add-in that connects to the WebSocket endpoint, subscribes to delta frames, and writes values into Excel ranges in real time. Works on Windows, Mac, and Excel Online — no COM registration required.

This is a separate project from the core Rust engine. It depends on the WebSocket endpoint and `/api/snapshot` being stable. No engine changes required.

### Python client library

A thin Python package (`hydrocube-python`) wrapping the REST and WebSocket APIs. Intended for use in Jupyter notebooks, data science workflows, and scripted integrations. Returns results as pandas DataFrames.

No engine changes required.

---

## What this deliberately does not include

- **gRPC streaming** — a natural future output for high-performance service-to-service use cases; deferred from v1
- **MQTT QoS 1/2** — at-least-once and exactly-once delivery; adds state management complexity; deferred
- **Webhook signature verification** — HMAC signing of outbound payloads for receiver verification; useful for security-sensitive endpoints; deferred
- **Kafka exactly-once semantics** — transactional producers; adds significant complexity; deferred
- **Push to S3/GCS** — writing window snapshots to object storage; the Parquet export in `retention.rs` covers local archival; cloud storage deferred

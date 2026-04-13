# HydroCube Development Guide

## Build & Test
- `cargo build` — build (requires librdkafka or use `--no-default-features` to skip Kafka)
- `cargo build --no-default-features` — build without Kafka (for dev without librdkafka installed)
- `cargo test` — run all tests
- `cargo test --test <name>` — run a specific integration test
- `cargo run -- --config cube.example.yaml --validate` — validate a config file

## Architecture
- All DuckDB access goes through `db_manager.rs` (channel-based, single connection)
- The hot path (window flush + aggregate + delta detect) runs on a tokio task
- Compaction runs on a separate tokio task, sends SQL to the DB manager
- SSE delivery uses `tokio::broadcast` for fan-out
- Lua transforms use batch contract (`transform_batch`) for performance

## Conventions
- Error types in `src/error.rs` — use `HcError` variants, propagate with `?`
- Config structs in `src/config.rs` — all serde-deserializable from YAML
- Logging via `tracing` macros (`info!`, `debug!`, `warn!`, `error!`) with target tags
- Tests use `tempfile` for DuckDB databases — no persistent test state

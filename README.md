# HydroCube

Real-time aggregation engine — streams incoming data into DuckDB, continuously
aggregates it in configurable time windows, detects row-level deltas, and
serves live results over HTTP/SSE.

## Quick Start

```bash
# Validate a config file
cargo run --no-default-features -- --config cube.example.yaml --validate

# Run with an example config (in-memory DB)
cargo run --no-default-features -- --config cube.example.yaml

# Reset the database and exit
cargo run --no-default-features -- --config cube.example.yaml --reset

# Rebuild from Parquet history
cargo run --no-default-features -- --config cube.example.yaml --rebuild
```

## Build

```bash
# Build without Kafka (no librdkafka required — fast dev builds)
cargo build --no-default-features

# Build with Kafka (statically linked — requires cmake)
cargo build

# Release build
cargo build --release
```

## Test

```bash
# Run all tests (no Kafka required)
cargo test --no-default-features

# Run a specific integration test
cargo test --no-default-features --test integration_test
```

## Design

See [docs/superpowers/specs/2026-04-13-hydrocube-design.md](docs/superpowers/specs/2026-04-13-hydrocube-design.md) for the full design specification.

#!/bin/bash
# run-test-env.sh — Start the full HydroCube test environment.
#
# Starts Kafka, builds HydroCube, launches it, and runs the trade generator.
# Ctrl+C tears everything down cleanly.
#
# Usage:
#   ./scripts/run-test-env.sh              # default: 50 trades/sec
#   ./scripts/run-test-env.sh --rate 200   # faster
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

RATE=50
while [ $# -gt 0 ]; do
    case "$1" in
        --rate) RATE="${2:-50}"; shift 2 ;;
        *)      RATE="$1"; shift ;;
    esac
done

# Colours
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log()  { echo -e "${GREEN}[test-env]${NC} $*"; }
warn() { echo -e "${YELLOW}[test-env]${NC} $*"; }
err()  { echo -e "${RED}[test-env]${NC} $*" >&2; }

# Track background PIDs for cleanup
PIDS=()
cleanup() {
    echo ""
    log "Shutting down..."
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
    log "Stopping Kafka..."
    docker compose down --timeout 5 2>/dev/null || true
    log "Done."
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------
log "Pre-flight checks..."

if ! command -v docker &>/dev/null; then
    err "docker not found. Install Docker Desktop."
    exit 1
fi

if ! command -v cargo &>/dev/null; then
    err "cargo not found. Install Rust: https://rustup.rs"
    exit 1
fi

if ! command -v python3 &>/dev/null; then
    err "python3 not found."
    exit 1
fi

# ---------------------------------------------------------------------------
# 1. Start Kafka
# ---------------------------------------------------------------------------
log "Starting Kafka (KRaft mode, no ZooKeeper)..."
docker compose up -d

log "Waiting for Kafka broker to be ready..."
RETRIES=30
for i in $(seq 1 $RETRIES); do
    if docker exec hydrocube-kafka kafka-topics.sh \
        --bootstrap-server localhost:9092 --list &>/dev/null; then
        log "Kafka is ready."
        break
    fi
    if [ "$i" -eq "$RETRIES" ]; then
        err "Kafka not ready after ${RETRIES}s. Check: docker compose logs kafka"
        exit 1
    fi
    sleep 1
done

# Create topic (idempotent)
docker exec hydrocube-kafka kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --topic trades.executed \
    --partitions 1 --replication-factor 1 \
    --if-not-exists 2>/dev/null || true
log "Topic 'trades.executed' ready."

# ---------------------------------------------------------------------------
# 2. Build HydroCube
# ---------------------------------------------------------------------------
log "Building HydroCube (with Kafka support)..."
cargo build 2>&1 | tail -3
log "Build complete."

# ---------------------------------------------------------------------------
# 3. Prepare local data directory
# ---------------------------------------------------------------------------
mkdir -p .local/slices

# Reset if previous state exists (clean test each time)
if [ -f .local/positions.db ]; then
    warn "Removing previous test state (.local/positions.db)"
    rm -f .local/positions.db .local/positions.db.wal
fi

# ---------------------------------------------------------------------------
# 4. Install Python deps
# ---------------------------------------------------------------------------
if ! python3 -c "import kafka" 2>/dev/null; then
    log "Installing kafka-python..."
    pip3 install --quiet kafka-python
fi

# ---------------------------------------------------------------------------
# 5. Start HydroCube
# ---------------------------------------------------------------------------
echo ""
log "${CYAN}Starting HydroCube...${NC}"
log "  Config:  cube.test.yaml"
log "  UI:      http://localhost:8080"
log "  Status:  http://localhost:8080/api/status"
log "  SSE:     http://localhost:8080/api/stream"
echo ""

cargo run -- --config cube.test.yaml &
HC_PID=$!
PIDS+=("$HC_PID")

# Give HydroCube a moment to start and create tables
sleep 3

# Check it's still running
if ! kill -0 "$HC_PID" 2>/dev/null; then
    err "HydroCube exited immediately. Check output above."
    exit 1
fi

# ---------------------------------------------------------------------------
# 6. Start trade generator
# ---------------------------------------------------------------------------
log "${CYAN}Starting trade generator (${RATE} trades/sec)...${NC}"
echo ""

python3 tools/generate_trades.py --rate "$RATE" &
GEN_PID=$!
PIDS+=("$GEN_PID")

# ---------------------------------------------------------------------------
# 7. Wait
# ---------------------------------------------------------------------------
echo ""
log "=========================================="
log "  Test environment is running!"
log ""
log "  UI:        ${CYAN}http://localhost:8081${NC}"
log "  Status:    ${CYAN}http://localhost:8081/api/status${NC}"
log "  Snapshot:  ${CYAN}http://localhost:8081/api/snapshot${NC}"
log "  SSE:       ${CYAN}curl -N http://localhost:8081/api/stream${NC}"
log ""
log "  Trade rate: ${RATE}/sec"
log "  Press Ctrl+C to stop everything."
log "=========================================="
echo ""

# Block until Ctrl+C (the trap handler cleans up)
wait 2>/dev/null || true

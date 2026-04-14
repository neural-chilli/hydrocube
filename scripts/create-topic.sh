#!/bin/bash
# Create the trades.executed topic in the local Kafka broker.
# Requires: docker compose up -d (Kafka must be running)
set -euo pipefail

BROKER="localhost:9092"
TOPIC="trades.executed"
RETRIES=30

echo "Waiting for Kafka broker at $BROKER..."
for i in $(seq 1 $RETRIES); do
    if docker exec hydrocube-kafka kafka-topics.sh \
        --bootstrap-server "$BROKER" --list >/dev/null 2>&1; then
        echo "Kafka is ready."
        break
    fi
    if [ "$i" -eq "$RETRIES" ]; then
        echo "ERROR: Kafka not ready after $RETRIES attempts"
        exit 1
    fi
    sleep 1
done

echo "Creating topic: $TOPIC"
docker exec hydrocube-kafka kafka-topics.sh \
    --bootstrap-server "$BROKER" \
    --create \
    --topic "$TOPIC" \
    --partitions 1 \
    --replication-factor 1 \
    --if-not-exists

echo ""
echo "Topics:"
docker exec hydrocube-kafka kafka-topics.sh \
    --bootstrap-server "$BROKER" --list

echo ""
echo "Done. Topic '$TOPIC' is ready."

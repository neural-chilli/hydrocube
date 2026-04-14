#!/usr/bin/env python3
"""
Rates desk trade generator for HydroCube testing.

Produces realistic JSON trade messages to a Kafka topic matching the
cube.example.yaml schema (trade_id, book, desk, instrument, instrument_type,
currency, quantity, price, notional, side, trade_time).

Usage:
    pip install kafka-python
    python generate_trades.py --broker localhost:9092 --topic trades.executed --rate 50
"""

import argparse
import json
import random
import sys
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------

BOOKS = [
    {"book": "Rates_NY",  "desk": "Rates"},
    {"book": "Rates_LDN", "desk": "Rates"},
    {"book": "Rates_TKY", "desk": "Rates"},
    {"book": "Rates_SG",  "desk": "Rates"},
]

INSTRUMENTS = [
    # instrument, instrument_type, currency, price_mid, price_spread
    ("UST2Y",   "Bond", "USD",  99.50, 0.50),
    ("UST5Y",   "Bond", "USD",  98.00, 1.00),
    ("UST10Y",  "Bond", "USD",  96.50, 2.00),
    ("UST30Y",  "Bond", "USD",  92.00, 4.00),
    ("GILT10Y", "Bond", "GBP", 100.50, 1.50),
    ("BUND10Y", "Bond", "EUR",  99.00, 1.00),
    ("JGB10Y",  "Bond", "JPY", 100.20, 0.30),
]

SIDES = ["BUY", "SELL"]

QUANTITY_MIN = 100
QUANTITY_MAX = 10_000
QUANTITY_STEP = 100

# ---------------------------------------------------------------------------
# Trade generation
# ---------------------------------------------------------------------------

def generate_trade() -> dict:
    """Generate a single realistic rates desk trade."""
    book_info = random.choice(BOOKS)
    inst_name, inst_type, ccy, price_mid, price_spread = random.choice(INSTRUMENTS)

    quantity = random.randrange(QUANTITY_MIN, QUANTITY_MAX + 1, QUANTITY_STEP)
    price = round(price_mid + random.uniform(-price_spread, price_spread), 4)
    notional = round(quantity * price, 2)
    side = random.choice(SIDES)

    return {
        "trade_id": str(uuid.uuid4()),
        "book": book_info["book"],
        "desk": book_info["desk"],
        "instrument": inst_name,
        "instrument_type": inst_type,
        "currency": ccy,
        "quantity": float(quantity),
        "price": price,
        "notional": notional,
        "side": side,
        "trade_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
    }

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="HydroCube rates desk trade generator")
    parser.add_argument("--broker", default="localhost:9092", help="Kafka broker address")
    parser.add_argument("--topic", default="trades.executed", help="Kafka topic")
    parser.add_argument("--rate", type=int, default=50, help="Trades per second (default: 50)")
    parser.add_argument("--duration", type=int, default=0, help="Seconds to run (0 = infinite)")
    args = parser.parse_args()

    print(f"Connecting to Kafka broker: {args.broker}")
    print(f"Topic: {args.topic}  |  Target rate: {args.rate}/s")
    print()

    producer = KafkaProducer(
        bootstrap_servers=args.broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks=1,
        linger_ms=5,
        batch_size=16384,
    )

    total_sent = 0
    interval_sent = 0
    interval_start = time.monotonic()
    stats_interval = 5.0  # Print stats every N seconds
    last_stats = time.monotonic()
    burst_interval = 10.0  # Burst every N seconds
    last_burst = time.monotonic()
    start_time = time.monotonic()

    sleep_per_msg = 1.0 / args.rate if args.rate > 0 else 0

    try:
        while True:
            # Check duration limit
            if args.duration > 0 and (time.monotonic() - start_time) >= args.duration:
                break

            # Burst mode: 5x rate for 1 second every burst_interval
            now = time.monotonic()
            if now - last_burst >= burst_interval:
                burst_count = args.rate * 5
                for _ in range(burst_count):
                    trade = generate_trade()
                    producer.send(args.topic, value=trade)
                    total_sent += 1
                    interval_sent += 1
                last_burst = now
                continue

            # Normal: generate one trade
            trade = generate_trade()
            producer.send(args.topic, value=trade)
            total_sent += 1
            interval_sent += 1

            # Stats
            if now - last_stats >= stats_interval:
                elapsed = now - interval_start
                actual_rate = interval_sent / elapsed if elapsed > 0 else 0
                print(
                    f"[{total_sent:>8} sent]  "
                    f"rate: {actual_rate:>6.0f}/s  "
                    f"elapsed: {now - start_time:>6.0f}s"
                )
                interval_sent = 0
                interval_start = now
                last_stats = now

            # Throttle
            if sleep_per_msg > 0:
                time.sleep(sleep_per_msg)

    except KeyboardInterrupt:
        print(f"\nStopping. Total sent: {total_sent}")
    finally:
        producer.flush()
        producer.close()
        print(f"Producer closed. Total messages: {total_sent}")


if __name__ == "__main__":
    main()

"""Phase 5 — filters new_orders for high-value orders (amount > 50),
prints them to the console, and appends them to a local JSON Lines file
as a stand-in for a micro-batch Silver landing zone.

JSON Lines (one object per line, append-only) rather than a single JSON
array — appending to a JSON array would mean re-reading and rewriting the
whole file on every match, and line-delimited JSON is what a real
landing zone would use anyway since it's natively loadable by
spark.read.json().

Exits once no new message has arrived for IDLE_TIMEOUT_SECONDS, so a run
terminates on its own rather than polling forever.
"""

import json
import os

from confluent_kafka import Consumer

TOPIC = "new_orders"
HIGH_VALUE_THRESHOLD = 50
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "filtered_orders.json")
IDLE_TIMEOUT_SECONDS = 15
POLL_INTERVAL_SECONDS = 1.0
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")

consumer = Consumer(
    {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "new_orders_silver_landing",
        "auto.offset.reset": "earliest",
    }
)
consumer.subscribe([TOPIC])

matched_count = 0
idle_seconds = 0.0

try:
    with open(OUTPUT_PATH, "a") as landing_zone:
        while idle_seconds < IDLE_TIMEOUT_SECONDS:
            msg = consumer.poll(POLL_INTERVAL_SECONDS)

            if msg is None:
                idle_seconds += POLL_INTERVAL_SECONDS
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            idle_seconds = 0.0
            order = json.loads(msg.value())

            if order["order_amount"] > HIGH_VALUE_THRESHOLD:
                matched_count += 1
                print(f"High-value order: {order}")
                landing_zone.write(json.dumps(order) + "\n")
finally:
    consumer.close()

print(f"{matched_count} high-value orders written to {OUTPUT_PATH}")

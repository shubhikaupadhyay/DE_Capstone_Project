"""Phase 5 — simulates 100 order events onto the new_orders topic, 1/sec.

Broker is a local Redpanda container (see docker-compose.yml) — no auth
needed. KAFKA_BOOTSTRAP_SERVERS defaults to the compose file's advertised
external listener; override it if pointing at a different broker.
"""

import json
import os
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

TOPIC = "new_orders"
EVENT_COUNT = 100
EVENT_INTERVAL_SECONDS = 1
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})


def _delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")


def _random_order_event() -> dict:
    return {
        "customer_id": f"cust_{random.randint(1000, 9999)}",
        "product_id": f"prod_{random.randint(100, 999)}",
        "order_amount": round(random.uniform(5, 200), 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


for _ in range(EVENT_COUNT):
    event = _random_order_event()
    producer.produce(TOPIC, value=json.dumps(event), callback=_delivery_report)
    producer.poll(0)
    time.sleep(EVENT_INTERVAL_SECONDS)

producer.flush()

# kafka/

A small real-time streaming demo, independent of the batch pipeline in
`../notebooks/` and `../dags/`. A producer simulates a live order feed
onto a Kafka topic; a consumer filters it down to the orders worth
paying attention to.

## What's here

| File | Does |
|---|---|
| `docker-compose.yml` | A single local Redpanda broker — Kafka-protocol compatible, no cloud account needed |
| `producer.py` | Simulates 100 order events onto the `new_orders` topic, one per second |
| `consumer.py` | Filters `new_orders` for anything over $50, prints matches, and appends them to `filtered_orders.json` |
| `filtered_orders.json` | is a run artifact — it's recreated/appended to every time `consumer.py` runs. |

## Flow

```
producer.py  →  new_orders topic  →  consumer.py  →  filtered_orders.json
                                          ↓
                                   console output
```

Each event carries a `customer_id`, `product_id`, `order_amount`, and
`timestamp`. The consumer treats `filtered_orders.json` as a stand-in
for a micro-batch landing zone — one JSON object per line, so it's
directly loadable by something like `spark.read.json()` without any
reformatting.

## Prerequisites & commands

Docker running locally.

```bash
docker compose -f kafka/docker-compose.yml up -d      # start the local Redpanda broker

docker exec redpanda rpk topic create new_orders       # create the topic

python producer.py                                     # send 100 simulated orders

python consumer.py                                     # filter + log high-value orders

docker compose -f kafka/docker-compose.yml down -v     # tear down when done
```


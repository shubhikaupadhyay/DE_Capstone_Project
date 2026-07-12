# Olist E-Commerce Data Pipeline

An end-to-end data engineering pipeline built on the [Olist Brazilian
E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce):
a Medallion (Bronze/Silver/Gold) lakehouse on Databricks, orchestrated by
Airflow, with automated data quality checks and a real-time Kafka streaming
demo alongside the batch path.

## Tech stack

| Concern | Tool | Notes |
|---|---|---|
| Storage & compute | Databricks + Delta Lake | Bronze/Silver/Gold tables, PySpark DataFrame API |
| Orchestration | Apache Airflow (Astronomer) | Local via Astro CLI + Docker |
| Data quality | Great Expectations | Expectation suite + HTML Data Docs |
| Streaming | Apache Kafka (Confluent Cloud) | Producer/consumer demo, independent of the batch DAG |
| Source data | Olist Brazilian E-Commerce dataset (Kaggle) | 9 raw CSVs |

## Architecture

**Batch path:** Kaggle CSVs → Bronze Delta (raw + audit columns) → Silver
Delta (cleansed, deduped, joined) → Gold Delta (business aggregates) →
Great Expectations validation, all orchestrated by an Airflow DAG. Airflow
itself runs locally (Astro CLI + Docker); the Spark/Delta/GX work all
happens in Databricks notebooks, which the DAG triggers remotely via the
Databricks Jobs API and blocks on until each run finishes.

**Streaming path** (independent of the batch DAG): a Kafka producer
simulates order events into a Confluent Cloud topic; a consumer filters
high-value orders and writes them to a local JSON file.

## Data model

Bronze ingests all 9 Olist CSVs (orders, customers, order items, products,
payments, reviews, sellers, geolocation, category translation) even though
only orders/customers/products are named in the source brief — the Gold
aggregates need order items and payments too. `order_amount` isn't a raw
column; it's derived from `order_payments` (sum of `payment_value` per
order) or `order_items` (`price + freight_value`).

Silver joins orders + customers + order items + products + payments into a
denormalized base table, deduped on `order_id`. Gold produces three
aggregates: daily revenue by product category, top 10 customers by
lifetime value, and a 7-day rolling average of order volume — all written
with `ZORDER BY` on the date column.

## Repo structure

```
notebooks/             Databricks notebooks (bronze.ipynb, silver.ipynb, gold.ipynb,
                        great_expectations.ipynb) — synced to Databricks via a
                        Git folder connection
great_expectations/     the suite runs inside notebooks/great_expectations.ipynb against the
                        in-cluster Spark table 
dags/                   Astro project — dags/dags/ecommerce_pipeline_dag.py +
                        dags/include/databricks_utils.py are hand-written;
                        the rest of the scaffold comes from `astro dev init`
kafka/                  producer.py, consumer.py
docs/                   architecture diagram + rubric screenshots
```

## Setup

1. **Databricks**: create a workspace, connect this repo as a Git folder
   (Repos), point it at `notebooks/`.
2. **Astro CLI**: `cd dags && astro dev init && astro dev start` (requires
   Docker running locally). Needs its own `dags/.env` with
   `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_NOTEBOOKS_ROOT`
3. **Confluent Cloud**: create a cluster + topic `new_orders`, put the
   bootstrap server and API key in a local `.env` (see `.env.example`,
   never commit the real one).
4. **Python deps**: `pip install -r requirements.txt` (great_expectations,
   confluent-kafka, python-dotenv, pyspark if running anything locally).

## Running the pipeline

| Stage | How to run |
|---|---|
| Bronze ingestion | Run `notebooks/bronze.ipynb` in Databricks |
| Silver / Gold transforms | Run `notebooks/silver.ipynb` then `notebooks/gold.ipynb` |
| Data quality checks | Run `notebooks/great_expectations.ipynb` in Databricks |
| Airflow DAG | `astro dev start`, trigger `ecommerce_pipeline` from the UI |
| Kafka streaming demo | `python kafka/producer.py` then `python kafka/consumer.py` |

The DAG (`ecommerce_pipeline`, `@daily`, 2 retries) chains
`ingest_bronze → transform_silver → run_dq_checks`, then branches on the
Great Expectations checkpoint result to `build_gold` or `quarantine`,
converging on `notify_complete`.


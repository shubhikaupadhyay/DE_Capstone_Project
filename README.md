# Olist E-Commerce Data Pipeline

An end-to-end data engineering pipeline built around the [Olist Brazilian
E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
— nine raw CSVs turned into a governed, validated Lakehouse, plus a
real-time streaming layer running independently alongside it.

The batch side follows a Bronze → Silver → Gold medallion architecture on
Databricks, data quality is enforced with Great Expectations before
anything reaches the Gold layer, and the whole thing is orchestrated
end-to-end by an Airflow DAG that branches on the quality check's result.
A separate Kafka producer/consumer demo simulates a live order feed
alongside the batch pipeline.

## Architecture

![Architecture diagram](docs/architecture.png)

## Tech stack

| Concern | Tool | Notes |
|---|---|---|
| Storage & compute | Databricks + Delta Lake | Bronze/Silver/Gold tables, PySpark DataFrame API, Unity Catalog volume for raw data |
| Data quality | Great Expectations | Suite + checkpoint against Silver, HTML Data Docs |
| Orchestration | Apache Airflow (Astronomer) | Runs locally via Astro CLI + Docker, triggers Databricks notebooks over the Jobs API |
| Streaming | Apache Kafka (Redpanda, local) | Producer/consumer demo, independent of the batch DAG |
| Source data | Olist Brazilian E-Commerce dataset (Kaggle) | 9 raw CSVs |

## How it works

**Batch path.** Nine raw CSVs land in a Unity Catalog volume, get ingested
into Bronze Delta tables with audit columns, then get cleaned,
deduplicated, and joined into a single Silver orders table. Great
Expectations validates that table — not-null keys, positive order
amounts, valid date ranges, and a referential-integrity check against the
customers table — before three Gold aggregates get built: daily revenue
by category, top 10 customers by lifetime value, and a 7-day rolling
average of order volume.

None of this runs on its own schedule. An Airflow DAG (`ecommerce_pipeline`)
owns the whole sequence: `ingest_bronze → transform_silver → run_dq_checks`,
branching to `build_gold` if the quality checks pass or `quarantine` if
they don't, and converging on `notify_complete` either way. Airflow itself
runs locally in Docker; every Databricks-side step is actually a remote
Jobs API call that Airflow submits and blocks on, so the orchestration
layer and the compute layer stay fully decoupled.

**Streaming path**, independent of all of the above: a producer simulates
100 order events onto a `new_orders` Kafka topic at one per second, and a
consumer filters out anything over $50, prints it to the console, and
lands it in a local JSON file — a stand-in for a micro-batch landing zone.

## Data model

Olist splits what a generic e-commerce brief would call "orders, products,
customers" across nine separate files. A few things worth knowing before
touching the code:

- `order_amount` isn't a column anywhere in the raw data — Silver derives
  it by summing `payment_value` per order from the payments file.
- `customer_id` is effectively one-per-order in this dataset;
  `customer_unique_id` is the actual customer identity, which is what
  lifetime value gets computed against.
- Bronze ingests all 9 CSVs, not just the 3 a generic brief would name,
  because the Gold aggregates need order items and payments too.

## Repo structure

```
notebooks/    Databricks notebooks — 01_bronze, 02_silver, 03_gold,
              great_expectations — synced to Databricks via a Git folder
              connection
dags/         Astro project — the ecommerce_pipeline DAG and the
              Databricks Jobs API client it runs on
kafka/        Local Redpanda broker (docker-compose) + producer/consumer
docs/         Architecture diagram and result screenshots
gx_data_docs/ The actual Great Expectations Data Docs site, checked in
              so the validation results are browsable
```

Each folder has its own `README.md` (what's in it and how it fits
together & exact steps to get it running).

## Getting started

1. **Databricks** — connect this repo to a workspace as a Git folder,
   pointed at `notebooks/`. Raw CSVs go in a Unity Catalog volume.
2. **Airflow** — `cd dags && astro dev init && astro dev start` (needs
   Docker). See `dags/SETUP.md` for the Databricks connection details it
   needs.
3. **Kafka** — `docker compose -f kafka/docker-compose.yml up -d`, then
   create the topic. See `kafka/SETUP.md`.

## Results

**Bronze schema**, after ingesting all 9 CSVs to Delta with audit columns:

![Bronze schema](docs/bronze_schema.png)

**Great Expectations Data Docs**, validating the Silver orders table:

![Great Expectations Data Docs](docs/gx_data_docs.png)

**A successful Airflow run** of the full `ecommerce_pipeline` DAG:

![Airflow DAG run](docs/dag.png)

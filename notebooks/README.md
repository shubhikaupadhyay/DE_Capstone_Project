# notebooks/

Databricks notebooks that build the Bronze/Silver/Gold lakehouse and
validate it, in `.ipynb` (Jupyter) format so they render with headings
and output directly on GitHub. They're synced to Databricks through a
Git folder connection — edit here, commit, push, and the workspace picks
up the latest version.

## What's here

| Notebook | Does |
|---|---|
| `01_bronze.ipynb` | Ingests all 9 raw Olist CSVs into Bronze Delta tables with schema inference and audit columns (`ingestion_timestamp`, `source_file`) |
| `02_silver.ipynb` | Cleanses, deduplicates, and joins Bronze into a single denormalized Silver orders table |
| `03_gold.ipynb` | Builds the three Gold aggregates — daily revenue by category, top 10 customers by lifetime value, 7-day rolling order volume |
| `great_expectations.ipynb` | Runs a Great Expectations suite against Silver orders and renders the HTML Data Docs |

## Flow

Each notebook reads the previous layer's output, so they run in a fixed
order:

```
01_bronze.ipynb  →  02_silver.ipynb  →  03_gold.ipynb
                            ↓
                  great_expectations.ipynb
```

`great_expectations.ipynb` validates `silver.orders` — not-null
`order_id`/`customer_id`, a positive `order_amount`, `order_date` inside
the dataset's known range, and a referential-integrity check that every
`customer_id` shows up in the customers table. In production this whole
sequence is what the Airflow DAG in `../dags/` runs on a schedule; these
notebooks are also the deliverables for running it by hand.

## Prerequisites

- A Databricks workspace with Unity Catalog enabled, connected to this
  repo as a Git folder pointed at `notebooks/`.
- Raw Olist CSVs uploaded to the Unity Catalog volume
  `de_capstone.default.olist_data` (`/Volumes/de_capstone/default/olist_data`)
  — this is the default `raw_path` widget value in `01_bronze.ipynb`.

## Running

Run in order: `01_bronze.ipynb` → `02_silver.ipynb` → `03_gold.ipynb` —
each reads the previous layer's tables (`silver.orders` reads from
`bronze.*`; `gold.*` reads from `silver.orders`). Run `great_expectations.ipynb` after `02_silver.ipynb` (it validates `silver.orders`).
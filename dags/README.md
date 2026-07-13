# dags/

An Astro (Airflow) project that orchestrates the batch pipeline.
Airflow itself runs locally in Docker — the actual data work happens in
Databricks, which this DAG reaches over the Jobs API.

## What's here

```
dags/dags/ecommerce_pipeline_dag.py    the DAG
dags/include/databricks_utils.py       shared Databricks Jobs API client
```

The rest of the folder (`Dockerfile`, `requirements.txt`,
`airflow_settings.yaml`, `plugins/`, `tests/`) is standard Astro project
scaffolding.

## Flow

`ecommerce_pipeline` runs `@daily` with 2 retries (5-minute delay) and
five tasks:

```
ingest_bronze → transform_silver → run_dq_checks ─┬→ build_gold ─┐
                                                  └→ quarantine ─┴→ notify_complete
```

`ingest_bronze`, `transform_silver`, and `build_gold` each submit a
one-off Databricks Jobs run for the corresponding notebook in
`../notebooks/` and block until it finishes. `run_dq_checks` does the
same for the Great Expectations notebook, then reads the checkpoint
result it returns to decide which branch to take — `build_gold` if the
data quality checks passed, `quarantine` if they didn't. Either branch
converges on `notify_complete`.

Because Airflow only ever submits and polls a remote job, none of the
actual Spark/Delta/Great Expectations work happens inside this Docker
container — it's a thin orchestration layer over Databricks.

## Prerequisites

- [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli)
  installed, with Docker running locally.
- A Databricks workspace already connected to this repo as a Git folder, with a personal access token.

## Setup

1. `cd dags && astro dev init` — safe to run even though
   `dags/dags/ecommerce_pipeline_dag.py` and `dags/include/databricks_utils.py`
   already exist; it only fills in the rest of the scaffold (Dockerfile,
   requirements.txt, etc.) without touching existing files.
2. Create `dags/.env` (separate from the repo-root `.env`, if you have
   one — Astro loads its own project-level `.env` automatically into the
   scheduler container) with:
   ```
   DATABRICKS_HOST=
   DATABRICKS_TOKEN=
   DATABRICKS_NOTEBOOKS_ROOT=
   ```
   `DATABRICKS_NOTEBOOKS_ROOT` is the workspace path to this repo's
   `notebooks/` folder as synced by your Databricks Git folder connection.
3. `astro dev start`.

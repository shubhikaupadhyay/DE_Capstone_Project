# dags/

`dags/dags/ecommerce_pipeline_dag.py` and `dags/include/databricks_utils.py`
are already written — see CLAUDE.md for the 5-task structure and branch
logic. What's not yet here is the rest of the Astro project scaffold
(Dockerfile, requirements.txt, etc.), since that needs the Astro CLI
(not available in this environment) to generate correctly.

## Setup

1. Install the [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli)
   if you haven't already, with Docker running locally.
2. `cd dags && astro dev init` — safe to run even with the DAG/include
   files already present; it only fills in the missing scaffold files
   (Dockerfile, requirements.txt, etc.) without touching existing ones.
3. Create `dags/.env` (separate from the repo-root `.env` used for
   Kafka — Astro loads its own project-level `.env` automatically into
   the scheduler container) with:
   ```
   DATABRICKS_HOST=
   DATABRICKS_TOKEN=
   DATABRICKS_NOTEBOOKS_ROOT=
   ```
   `DATABRICKS_NOTEBOOKS_ROOT` is the workspace path to this repo's
   `notebooks/` folder as synced by your Databricks Git folder connection
   (find it in the Databricks workspace file browser) — e.g.
   `/Workspace/Users/<you>/DE_Capstone_Project/notebooks`.
4. `astro dev start`.

## Running

Trigger `ecommerce_pipeline` manually from the Airflow UI
(`localhost:8080`). Take the **Gantt chart screenshot** deliverable from
a successful run — this has to be captured manually in the Airflow UI.

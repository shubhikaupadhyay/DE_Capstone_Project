"""ecommerce_pipeline — Bronze/Silver/Gold + Great Expectations, orchestrated
from Airflow but executed on Databricks.

Airflow runs locally (Astro CLI + Docker); the actual Spark/Delta work
happens in Databricks notebooks synced via Git folder. Each Databricks-side
task submits a one-off Jobs API run for its notebook and blocks until it
finishes (see include/databricks_utils.py). run_dq_checks reads the
great_expectations notebook's checkpoint result (returned via
dbutils.notebook.exit) to branch to build_gold or quarantine.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from include.databricks_utils import run_notebook

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Workspace notebook names (no .ipynb — Databricks addresses notebooks
# without the source extension), matching the git-tracked file names in
# ../../notebooks/.
BRONZE_NOTEBOOK = "01_bronze"
SILVER_NOTEBOOK = "02_silver"
GOLD_NOTEBOOK = "03_gold"
GREAT_EXPECTATIONS_NOTEBOOK = "great_expectations"


def _ingest_bronze():
    run_notebook(BRONZE_NOTEBOOK)


def _transform_silver():
    run_notebook(SILVER_NOTEBOOK)


def _run_dq_checks():
    result = run_notebook(GREAT_EXPECTATIONS_NOTEBOOK, fetch_output=True)
    return "build_gold" if result.get("success") else "quarantine"


def _build_gold():
    run_notebook(GOLD_NOTEBOOK)


def _quarantine():
    print("Data quality checks failed — quarantining this run's output.")


def _notify_complete():
    print("ecommerce_pipeline run complete.")


with DAG(
    dag_id="ecommerce_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
) as dag:
    ingest_bronze = PythonOperator(task_id="ingest_bronze", python_callable=_ingest_bronze)
    transform_silver = PythonOperator(
        task_id="transform_silver", python_callable=_transform_silver
    )
    run_dq_checks = BranchPythonOperator(
        task_id="run_dq_checks", python_callable=_run_dq_checks
    )
    build_gold = PythonOperator(task_id="build_gold", python_callable=_build_gold)
    quarantine = PythonOperator(task_id="quarantine", python_callable=_quarantine)
    notify_complete = PythonOperator(
        task_id="notify_complete",
        python_callable=_notify_complete,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    ingest_bronze >> transform_silver >> run_dq_checks >> [build_gold, quarantine] >> notify_complete

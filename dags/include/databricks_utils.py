import json
import os
import time

import requests

_TERMINAL_STATES = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}
_POLL_SECONDS = 15


def _headers() -> dict:
    return {"Authorization": f"Bearer {os.environ['DATABRICKS_TOKEN']}"}


def run_notebook(notebook_name: str, fetch_output: bool = False) -> dict:
    """Submit a one-off Databricks Jobs run for a notebook and block until it finishes.

    notebook_name is the bare notebook name (no .ipynb extension — Databricks
    addresses notebooks by workspace path without the source file extension,
    regardless of Jupyter vs. source format) relative to
    DATABRICKS_NOTEBOOKS_ROOT. No cluster/environment is specified in the
    task, so Databricks falls back to the notebook's own attached serverless
    environment.

    fetch_output only needs to be True for a notebook that actually calls
    dbutils.notebook.exit() (just great_expectations, for its checkpoint
    result). Returns the run's notebook output parsed as JSON, or an empty
    dict if fetch_output is False.
    """
    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    notebooks_root = os.environ["DATABRICKS_NOTEBOOKS_ROOT"].rstrip("/")

    submit_response = requests.post(
        f"{host}/api/2.1/jobs/runs/submit",
        headers=_headers(),
        json={
            "run_name": f"ecommerce_pipeline-{notebook_name}",
            "tasks": [
                {
                    "task_key": notebook_name,
                    "notebook_task": {"notebook_path": f"{notebooks_root}/{notebook_name}"},
                }
            ],
        },
    )
    submit_response.raise_for_status()
    run_id = submit_response.json()["run_id"]

    while True:
        run = requests.get(
            f"{host}/api/2.1/jobs/runs/get", headers=_headers(), params={"run_id": run_id}
        )
        run.raise_for_status()
        run_json = run.json()
        state = run_json["state"]
        if state["life_cycle_state"] in _TERMINAL_STATES:
            break
        time.sleep(_POLL_SECONDS)

    if state.get("result_state") != "SUCCESS":
        raise RuntimeError(f"Databricks run for '{notebook_name}' did not succeed: {state}")

    if not fetch_output:
        return {}

    # runs/get-output rejects the top-level (multi-task) run_id — the job
    # run is just a holder for its task runs and has no output of its own.
    # It needs the individual task's own run_id from the tasks array.
    task_run_id = run_json["tasks"][0]["run_id"]

    output_response = requests.get(
        f"{host}/api/2.1/jobs/runs/get-output",
        headers=_headers(),
        params={"run_id": task_run_id},
    )
    output_response.raise_for_status()
    result = output_response.json().get("notebook_output", {}).get("result")

    return json.loads(result) if result else {}

"""
Runs the dbt project (dbt_movielens) after each successful MovieLens ingestion.

- Triggered by the `movielens_raw` Asset, updated by the ingestion DAG.
- Runs `dbt build` as a single task: DuckDB runs in memory, so all models
  (including the in-memory staging views) must run in the same process.
- Sends a Slack message (incoming webhook) when the task fails.
"""

import os
from datetime import datetime, timedelta

import requests
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Asset, dag

DBT_PROJECT_DIR = "/usr/local/airflow/dbt_movielens"
DBT_EXECUTABLE = "/usr/local/airflow/dbt_venv/bin/dbt"

# Same name as the Asset declared in the ingestion DAG
MOVIELENS_RAW = Asset("movielens_raw")


def notify_slack_on_failure(context):
    """Posts a failure message to Slack. Does nothing if the webhook is not set."""
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        return

    ti = context["ti"]
    log_url = getattr(ti, "log_url", None)

    lines = [
        ":red_circle: *Airflow task failed*",
        f"*DAG:* `{ti.dag_id}`",
        f"*Task:* `{ti.task_id}`",
        f"*Run:* `{context.get('run_id')}`",
        f"*Try:* {ti.try_number}",
    ]
    if log_url:
        lines.append(f"<{log_url}|View logs>")

    try:
        requests.post(webhook_url, json={"text": "\n".join(lines)}, timeout=10)
    except requests.RequestException:
        # Never let a Slack outage break the callback
        pass


@dag(
    dag_id="dbt_movielens_build",
    schedule=[MOVIELENS_RAW],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "andre",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": notify_slack_on_failure,
    },
    tags=["dbt", "movielens", "iceberg"],
)
def dbt_movielens_build():
    BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"{DBT_EXECUTABLE} deps --profiles-dir . && "
            f"{DBT_EXECUTABLE} build --profiles-dir ."
        ),
        cwd=DBT_PROJECT_DIR,
        execution_timeout=timedelta(hours=1),
    )


dbt_movielens_build()
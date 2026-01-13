from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "fri",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="fraud_rev_intel_pipeline",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 */6 * * *",  # every 6 hours
    catchup=False,
    tags=["sql","dbt","clickhouse","resume"],
) as dag:

    ingest = BashOperator(
        task_id="generate_data",
        bash_command="python -m ingestion.generate --rows 3000",
        env={"PYTHONPATH": "/opt/project", **{}},
        cwd="/opt/project",
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="dbt deps && dbt build",
        cwd="/opt/project/warehouse/dbt",
        env={"DBT_PROFILES_DIR": "/opt/project/warehouse/dbt"},
    )

    ch_sync = BashOperator(
        task_id="sync_to_clickhouse",
        bash_command="python -m warehouse.sync_to_clickhouse",
        env={"PYTHONPATH": "/opt/project", **{}},
        cwd="/opt/project",
    )

    ingest >> dbt_build >> ch_sync

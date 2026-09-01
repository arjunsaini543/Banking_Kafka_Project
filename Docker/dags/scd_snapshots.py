from datetime import UTC, datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="SCD2_snapshots",
    default_args=default_args,
    description="Run dbt snapshots for SCD2",
    schedule="0 2 * * *",     
    start_date=datetime(2026, 8, 25, tzinfo=UTC),    catchup=False,
    tags=["dbt", "snapshots"],
) as dag:

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=(
            "cd /opt/airflow/banking_dbt && "
            "dbt snapshot --profiles-dir /home/airflow/.dbt"
        ),
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            "cd /opt/airflow/banking_dbt && "
            "dbt run --select marts --profiles-dir /home/airflow/.dbt"
        ),
    )

    dbt_snapshot >> dbt_run_marts
    
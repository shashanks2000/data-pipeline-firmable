from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="run_main_py",
    default_args=default_args,
    description="Run project main.py daily",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["project", "run-main"],
) as dag:

    cc_ingest = BashOperator(
        task_id="run_main_py",
        bash_command="python -u /opt/airflow/project/main.py 2>&1",
        env={"PYTHONPATH": "/opt/airflow/project"},
        execution_timeout=timedelta(hours=2),
    )

    abr_ingest_and_flatten = BashOperator(
        task_id="run_abr_pull_push_py",
        bash_command="python -u /opt/airflow/project/src/ingestion/abr_pull_push.py --workers 6 --batch 8000 2>&1",
        env={"PYTHONPATH": "/opt/airflow/project"},
        execution_timeout=timedelta(hours=4),
    )

    cc_flatten = BashOperator(
        task_id="run_cc_flatten_py",
        bash_command="python -u /opt/airflow/project/src/ingestion/cc_flatten.py 2>&1",
        env={"PYTHONPATH": "/opt/airflow/project"},
        execution_timeout=timedelta(hours=4),
    )

    cc_ingest >> abr_ingest_and_flatten >> cc_flatten

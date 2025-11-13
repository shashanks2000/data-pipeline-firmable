"""DAG to run the repository's main.py.

This DAG executes the project's `main.py` which is mounted into the Airflow
container at `/opt/airflow/project`. The task sets PYTHONPATH to that folder so
imports inside your project work as expected.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    dag_id='run_main_py',
    default_args=default_args,
    description='Run project main.py daily',
    start_date=datetime(2025, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['project', 'run-main'],
) as dag:

    run_main = BashOperator(
        task_id='run_main_script',
        # -u for unbuffered output so logs appear promptly in Airflow UI
        bash_command='python -u /opt/airflow/project/main.py',
        env={
            'PYTHONPATH': '/opt/airflow/project',
        },
        dag=dag,
    )

    run_main
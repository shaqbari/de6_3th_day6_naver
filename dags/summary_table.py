from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'yeanjun',
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='summary_dbt_dag',
    default_args=default_args,
    start_date=datetime(2025, 6, 1),
    schedule_interval=None,
    catchup=False,
    tags=['dbt'],
) as dag:

    run_dbt_summary = BashOperator(
        task_id='run_dbt_summary',
        bash_command="""
            /home/airflow/.local/bin/dbt run \
            --profiles-dir /opt/airflow/learn_dbt \
            --project-dir /opt/airflow/learn_dbt
        """
    )

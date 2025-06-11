from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.hooks.postgres_hook import PostgresHook
import os
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 5), # DAG 시작 날짜
    'retries': 1, # 실패 시 재시도 횟수
    'retry_delay': timedelta(minutes=2), # 재시도 간격
}

# dbt 프로젝트의 루트 경로 naver_project
DBT_PROJECT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'naver_project')

@task
def extract_data_to_staging():
    """
    네이버 쇼핑 원본 데이터를 추출하여 데이터베이스에 로드합니다.
    이 데이터는 dbt 모델의 소스로 활용됩니다.
    """
    hook = PostgresHook(postgres_conn_id='postgres_naver_conn')
    engine = hook.get_sqlalchemy_engine()

    try:
        # naver_shopping 테이블에서 모든 데이터를 읽음
        df = pd.read_sql("SELECT * FROM naver_shopping", con=engine)

        pass

    finally:

        pass

with DAG(
    dag_id='anomaly_detection', # DAG ID
    default_args=default_args,
    schedule_interval=None, 
    catchup=False, # 과거 누락된 DAG 실행 안 함
    tags=['naver', 'anomaly', 'dbt'], # DAG 태그
) as dag:


    #dbt 모델 실행 태스크
    #'summary_shop_keyword' 테이블과 'anomaly' 뷰를 생성하거나 업데이트
    run_dbt_models_task = BashOperator(
    task_id='run_dbt_models',
    bash_command=f"export PATH=$PATH:/home/airflow/.local/bin && cd {DBT_PROJECT_DIR} && dbt run --profiles-dir .",
    env={
        "POSTGRES_USER": os.environ.get("AIRFLOW_VAR_NAVER_DB_USER", "naver"),
        "POSTGRES_PASSWORD": os.environ.get("AIRFLOW_VAR_NAVER_DB_PASSWORD", "naver"),
        "POSTGRES_HOST": "postgres-naver", 
        "POSTGRES_DB": "naver",
        "POSTGRES_PORT": "5432"
    }
)

    #dbt 테스트 실행 태스크 
    run_dbt_tests_task = BashOperator(
    task_id='run_dbt_tests',
    bash_command=f"export PATH=$PATH:/home/airflow/.local/bin && cd {DBT_PROJECT_DIR} && dbt test --profiles-dir .",
    env={
        "POSTGRES_USER": os.environ.get("AIRFLOW_VAR_NAVER_DB_USER", "naver"),
        "POSTGRES_PASSWORD": os.environ.get("AIRFLOW_VAR_NAVER_DB_PASSWORD", "naver"),
        "POSTGRES_HOST": "postgres-naver",
        "POSTGRES_DB": "naver",
        "POSTGRES_PORT": "5432"
    }
)

    # 태스크 간 의존성 설정
    # dbt 모델 실행 -> dbt 테스트 실행 
    run_dbt_models_task >> run_dbt_tests_task
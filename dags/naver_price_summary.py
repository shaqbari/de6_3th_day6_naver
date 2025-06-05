from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from sqlalchemy import create_engine
from airflow.decorators import task
from datetime import datetime, timedelta
import requests
import pandas as pd
import json
import logging

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}


@task
def make_naver_price_summary(**kwargs):
    engine = create_engine(Variable.get('POSTGRE_NAVER_CONN'))

    query_sql = """
        SELECT product_id,
            MIN(CASE
                WHEN date_trunc('hour', dt) = date_trunc('hour', NOW() - interval '4 hours')
                    THEN lprice END) AS h_4_hours_ago,
            MIN(CASE
                WHEN date_trunc('hour', dt) = date_trunc('hour', NOW() - interval '3 hours')
                    THEN lprice END) AS h_3_hours_ago,
            MIN(CASE
                WHEN date_trunc('hour', dt) = date_trunc('hour', NOW() - interval '2 hours')
                    THEN lprice END) AS h_2_hours_ago,
            MIN(CASE
                WHEN date_trunc('hour', dt) = date_trunc('hour', NOW() - interval '1 hours')
                    THEN lprice END) AS h_1_hour_ago
        FROM naver_price
        WHERE dt >= NOW() - interval '5 hours'
        GROUP BY
            product_id
        ORDER BY
            product_id; \
    """

    df_summary_table = pd.read_sql(query_sql, con=engine)
    df_cleaned = df_summary_table.dropna()
    df_cleaned.set_index('product_id', inplace=True)

    # 전 시간 대비 변화율 (%) 계산
    pct_df = df_cleaned.pct_change(axis=1) * 100
    pct_df.fillna(0, inplace=True)

    # 이전보다 값이 떨어진 행만 필터링
    rows_with_change_minus = pct_df[(pct_df < 0).any(axis=1)]

    # 테이블 full refresh
    rows_with_change_minus.to_sql('naver_price_summary', con=engine, if_exists='replace', index=True)

    #삽입되었는지 확인, 나중에 삭제
    confirm_table = pd.read_sql("SELECT * FROM naver_price_summary", con=engine)
    print(confirm_table)
    print(confirm_table.info())


with DAG(
        dag_id='naver_price_summary',
        default_args=default_args,
        schedule_interval='5 * * * *',  # hourly
        catchup=False,
        tags=['naver', 'postgres'],
) as dag:
    # DAG 실행 순서 정의
    make_naver_price_summary()

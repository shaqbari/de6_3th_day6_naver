from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.exceptions import AirflowSkipException
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
import pandas as pd
import logging
import requests
import pytz
import re
import os

default_args = {
    'start_date': datetime(2025, 6, 9),
    'retries': 1,
    'retry_delay': timedelta(minutes=10)
}

def extract_from_dw(**context):
    hook = PostgresHook(postgres_conn_id='my_postgres_conn_id')
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.dm_tmp_filtered_price AS
        SELECT * FROM public.ndw WHERE false;
    """)
    conn.commit()

    cur.execute("TRUNCATE TABLE public.dm_tmp_filtered_price;")

    cur.execute("""
        INSERT INTO public.dm_tmp_filtered_price
        SELECT *
        FROM public.ndw
        WHERE productid IN (
            SELECT productid
            FROM public.ndw
            GROUP BY productid
            HAVING COUNT(DISTINCT lprice) > 1
        );
    """)
    conn.commit()

    cur.close()
    conn.close()
    logging.info("📦 DW → dm_tmp_filtered_price 테이블 최신화 완료")


def preprocess_dm_data(**context):
    hook = PostgresHook(postgres_conn_id='my_postgres_conn_id')
    engine = hook.get_sqlalchemy_engine()

    df = pd.read_sql("SELECT * FROM public.dm_tmp_filtered_price", engine)

    if df.empty:
        raise AirflowSkipException("⛔ 전처리할 데이터가 없습니다.")

    df['dt'] = pd.to_datetime(df['dt'], utc=True).dt.tz_convert('Asia/Seoul').dt.tz_localize(None)
    df.sort_values(by=['productid', 'dt'], inplace=True)

    summary_list = []

    for productid, group in df.groupby('productid'):
        group = group.sort_values('dt')
        first_row = group.iloc[0]
        last_row = group.iloc[-1]
        min_price = group['lprice'].min()
        max_price = group['lprice'].max()
        avg_price = int(group['lprice'].mean())

        min_price_dt = group[group['lprice'] == min_price]['dt'].max()
        max_price_dt = group[group['lprice'] == max_price]['dt'].max()

        summary = {
            'productid': productid,
            'title': re.sub(r'<[^>]+>', '', last_row['title']),
            'dt': last_row['dt'],
            'link': last_row['link'],
            'keyword': last_row['keyword'],
            'keyword_type': last_row['keyword_type'],
            'first_price': int(first_row['lprice']),
            'first_price_dt': first_row['dt'],
            'last_price': int(last_row['lprice']),
            'min_price': int(min_price),
            'min_price_dt': min_price_dt,
            'max_price': int(max_price),
            'max_price_dt': max_price_dt,
            'avg_price': avg_price,
            'price_diff': int(last_row['lprice'] - first_row['lprice']),
            'price_diff_pct': round((last_row['lprice'] - first_row['lprice']) / first_row['lprice'] * 100, 2) if first_row['lprice'] else 0.0
        }

        summary_list.append(summary)

    df_summary = pd.DataFrame(summary_list)
    df_summary.to_sql('dm_tmp_price_summary', engine, schema='public', if_exists='replace', index=False)
    logging.info(f"✅ 전처리 완료 → dm_tmp_price_summary 테이블 저장 ({len(df_summary)}건)")


def insert_dm_data(**context):
    hook = PostgresHook(postgres_conn_id='my_postgres_conn_id')
    conn = hook.get_conn()
    cur = conn.cursor()

    df = pd.read_sql("SELECT * FROM public.dm_tmp_price_summary", conn)

    if df.empty:
        logging.warning("⛔ DM 테이블에 적재할 데이터가 없습니다.")
        return

    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.ndm (
            productid TEXT PRIMARY KEY,
            title TEXT,
            dt TIMESTAMP,
            link TEXT,
            keyword TEXT,
            keyword_type TEXT,
            first_price BIGINT,
            first_price_dt TIMESTAMP,
            last_price BIGINT,
            min_price BIGINT,
            min_price_dt TIMESTAMP,
            max_price BIGINT,
            max_price_dt TIMESTAMP,
            avg_price BIGINT,
            price_diff BIGINT,
            price_diff_pct FLOAT
        );
    """)
    conn.commit()

    rows = df[[
        'productid', 'title', 'dt', 'link', 'keyword', 'keyword_type',
        'first_price', 'first_price_dt', 'last_price',
        'min_price', 'min_price_dt', 'max_price', 'max_price_dt',
        'avg_price', 'price_diff', 'price_diff_pct'
    ]].values.tolist()

    execute_values(cur, """
        INSERT INTO public.ndm (
            productid, title, dt, link, keyword, keyword_type,
            first_price, first_price_dt, last_price,
            min_price, min_price_dt, max_price, max_price_dt,
            avg_price, price_diff, price_diff_pct
        ) VALUES %s
        ON CONFLICT (productid) DO UPDATE SET
            title = EXCLUDED.title,
            dt = EXCLUDED.dt,
            link = EXCLUDED.link,
            keyword = EXCLUDED.keyword,
            keyword_type = EXCLUDED.keyword_type,
            first_price = EXCLUDED.first_price,
            first_price_dt = EXCLUDED.first_price_dt,
            last_price = EXCLUDED.last_price,
            min_price = EXCLUDED.min_price,
            min_price_dt = EXCLUDED.min_price_dt,
            max_price = EXCLUDED.max_price,
            max_price_dt = EXCLUDED.max_price_dt,
            avg_price = EXCLUDED.avg_price,
            price_diff = EXCLUDED.price_diff,
            price_diff_pct = EXCLUDED.price_diff_pct;
    """, rows)

    conn.commit()
    cur.close()
    conn.close()
    logging.info(f"✅ DM 테이블(dm_naver_price)에 {len(df)}건 적재 완료")

def send_slack_message(message: str):
    SLACK_URL = Variable.get("SLACK_WEBHOOK_URL")
    requests.post(SLACK_URL, json={"text": message})

def alert_slack_task(**kwargs):
    hook = PostgresHook(postgres_conn_id='my_postgres_conn_id')
    engine = hook.get_sqlalchemy_engine()

    df = pd.read_sql("SELECT * FROM ndm", con=engine)

    if df.empty:
        print("⚠️ 알림 대상 상품 없음.")
        return

    filtered_df = df[
        (df['last_price'] <= df['max_price'] * 0.8) |
        (df['last_price'] <= df['avg_price'] * 0.8)
    ]

    if filtered_df.empty:
        print("⚠️ 조건을 만족하는 상품이 없습니다.")
        return

    for _, row in filtered_df.iterrows():
        max_drop = -(row['max_price'] - row['last_price'])
        max_drop_pct = (max_drop / row['max_price']) * 100

        msg = f"""📢 *[{row['title']}]*  
🔗 <{row['link']}|상품 보러가기>  
🛒 키워드: {row['keyword']} / {row['keyword_type']}  
🕘 분석 기준 시점: {pd.to_datetime(row['dt']).strftime('%Y-%m-%d')}  

💰 최초가: {row['first_price']:,}원 ({pd.to_datetime(row['first_price_dt']).strftime('%Y-%m-%d')})  
📉 최저가: {row['min_price']:,}원 ({pd.to_datetime(row['min_price_dt']).strftime('%Y-%m-%d')})  
📈 최고가: {row['max_price']:,}원 ({pd.to_datetime(row['max_price_dt']).strftime('%Y-%m-%d')})  
🧮 평균가: {row['avg_price']:,}원  
💸 현재가: {row['last_price']:,}원  

🔻 최초가 대비: {row['price_diff']:,}원 ({row['price_diff_pct']:.2f}%)  
🔻 최고가 대비: {max_drop:,}원 ({max_drop_pct:.2f}%)
────────────────────────────────────────────────────────────────────────
"""
        send_slack_message(msg)

with DAG(
    dag_id='dm_dag',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
    tags=['dm', 'dw_to_dm', 'table_load']
) as dag:

    t1 = ExternalTaskSensor(
        task_id='wait_for_dw_dag',
        external_dag_id='dw_dag',
        external_task_id='transfer_to_dw',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        execution_delta=timedelta(hours=0),
        mode='reschedule',
        poke_interval=60,
        timeout=600
    )

    t2 = PythonOperator(
        task_id='extract_from_dw',
        python_callable=extract_from_dw,
    )

    t3 = PythonOperator(
        task_id='preprocess_dm_data',
        python_callable=preprocess_dm_data,
    )

    t4 = PythonOperator(
        task_id='insert_dm_data',
        python_callable=insert_dm_data,
    )


    t5 = PythonOperator(
        task_id='send_slack_alert',
        python_callable=alert_slack_task,
    )

    t1 >> t2 >> t3 >> t4 >> t5

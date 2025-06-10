from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.hooks.postgres_hook import PostgresHook
from sqlalchemy.sql import text
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

BASE_SQL_PATH = r'D:\docker\de6_3th_day6_naver\naver_project\models\marts'

def load_sql(filename: str) -> str:
    with open(os.path.join(BASE_SQL_PATH, filename), 'r', encoding='utf-8') as file:
        return file.read()

@task
def detect_and_save_summary():
    hook = PostgresHook(postgres_conn_id='postgres_naver_conn')
    engine = hook.get_sqlalchemy_engine()

    df = pd.read_sql("SELECT * FROM naver_shopping", con=engine)
    df['dt'] = pd.to_datetime(df['dt'])
    df['hour'] = df['dt'].dt.floor('h')
    df_filtered = df[df['keyword'].notnull()]

    result = df_filtered.groupby(['hour', 'keyword'])['lprice'].agg(
        avg_price='mean',
        min_price='min',
        max_price='max'
    ).reset_index()

    result['avg_price'] = result['avg_price'].astype(int)
    result['id'] = result['hour'].astype(str)
    result = result[['id', 'keyword', 'avg_price', 'min_price', 'max_price']]

    with engine.begin() as conn:
        # 테이블 생성
        table_sql = load_sql('summary_shop_keyword.sql')
        conn.execute(text(table_sql))

        # 데이터 삽입
        if not result.empty:
            keys_to_delete = result[['id', 'keyword']].drop_duplicates()
            tuple_strs = ", ".join(
                f"('{row['id']}', '{row['keyword']}')" for _, row in keys_to_delete.iterrows()
            )
            delete_sql = f"""
                DELETE FROM summary_shop_keyword
                WHERE (id, keyword) IN ({tuple_strs})
            """
            conn.execute(text(delete_sql))
            result.to_sql('summary_shop_keyword', con=conn, if_exists='append', index=False)

        # 뷰 생성
        view_sql = load_sql('anomaly.sql')
        conn.execute(text(view_sql))

with DAG(
    dag_id='anomaly_detection',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['naver', 'anomaly'],
) as dag:
    detect_and_save_summary()

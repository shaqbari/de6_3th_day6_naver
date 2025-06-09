from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='anomaly_detection',
    default_args=default_args,
    schedule_interval=None,  # Trigger로만 실행됨
    catchup=False,
    tags=['anomaly', 'naver'],
) as dag:

    @task
    def detect_and_save():
        # DB 연결
        hook = PostgresHook(postgres_conn_id='postgres_naver_conn')
        engine = hook.get_sqlalchemy_engine()

        # 1. 전체 데이터 로드
        df = pd.read_sql("SELECT * FROM naver_shopping", con=engine)

        # 2. datetime 처리
        df['dt'] = pd.to_datetime(df['dt'])
        df['hour'] = df['dt'].dt.floor('h')

        # 3. 사용할 키워드 목록
        keywords = ['에어컨', '제습기', '선풍기', '서큘레이터', '아이스박스',
                    '가습기', '히터', '보온병', '온풍기', '전기매트']

        df_filtered = df[df['keyword'].isin(keywords)]

        # 4. 그룹화 및 가격 통계
        result = df_filtered.groupby(['hour', 'keyword'])['lprice'].agg(
            avg_price='mean',
            min_price='min',
            max_price='max'
        ).reset_index()

        result['avg_price'] = result['avg_price'].astype(int)
        result = result[['hour', 'keyword', 'avg_price', 'min_price', 'max_price']]

        # 5. id 컬럼 생성 (hour + keyword 기반)
        result['id'] = result['hour'].astype(str) + '_' + result['keyword']
        result = result[['id', 'keyword', 'avg_price', 'min_price', 'max_price']]

        print(result.head())

        # 6. 저장 (예: 테이블로 저장)
        result.to_sql('naver_anomaly_summary', con=engine, if_exists='replace', index=False)

    detect_and_save()

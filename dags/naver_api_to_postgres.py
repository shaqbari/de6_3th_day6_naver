from datetime import datetime, timedelta
import requests
import pandas as pd
import json
import time
import logging
from pytz import timezone

from airflow import DAG
from airflow.decorators import task
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

# Naver API 키는 Variable에서 가져옴 (환경 변수로도 가능)
from airflow.models import Variable
NAVER_CLIENT_ID = Variable.get('NAVER_API_CLIENT_ID')
NAVER_CLIENT_SECRET = Variable.get('NAVER_API_CLIENT_SECRET')

# 사용할 컬럼 정의
col_names = [
    'id', 'dt', 'keyword_type', 'keyword', 'brand', 'category1',
    'category2', 'category3', 'category4', 'hprice', 'image', 'link',
    'lprice', 'maker', 'mall_name', 'product_id', 'product_type', 'title'
]

# Naver API 요청 함수
def naver_search_shopping(query, display=100):
    url = "https://openapi.naver.com/v1/search/shop.json"
    headers = {
        "X-Naver-Client-Id": NAVER_CLIENT_ID,
        "X-Naver-Client-Secret": NAVER_CLIENT_SECRET,
    }
    params = {
        'query': query,
        'display': display,
        'sort': 'sim',
        'exclude': 'used:cbshop:rental'
    }

    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        print(f"조회 성공: {query}")
        return response.json()
    else:
        print(f"요청 실패: {response.status_code}")
        raise Exception(f"Naver API 요청 실패: {response.status_code}")

# 문자열을 정수로 변환
def str_to_int(s: str):
    s = str(s).strip()
    return int(s) if s else 0

# 데이터 처리 및 PostgreSQL 적재
@task
def load_via_sqlalchemy(**kwargs):
    keyword_dict = {
        '여름': ['제습기', '선풍기', '에어컨', '서큘레이터', '아이스박스'],
        '겨울': ['가습기', '히터', '보온병', '온풍기', '전기매트']
    }
    dfs = []

    for keyword_type, keywords in keyword_dict.items():
        for keyword in keywords:
            df_keyword = pd.DataFrame(naver_search_shopping(keyword)['items'])
            df_keyword['keyword_type'] = keyword_type
            df_keyword['keyword'] = keyword
            dfs.append(df_keyword)
            time.sleep(0.5)  # API 호출 간격

    df = pd.concat(dfs, ignore_index=True)
    df.rename(columns={
        'mallName': 'mall_name',
        'productId': 'product_id',
        'productType': 'product_type'
    }, inplace=True)

    store_dt = kwargs['logical_date'].naive()
    df['dt'] = store_dt
    df['id'] = df['product_id'].astype(str) + '_' + store_dt.strftime('%Y-%m-%d_%H')

    df['hprice'] = df['hprice'].apply(str_to_int)
    df['lprice'] = df['lprice'].apply(str_to_int)
    df['product_id'] = df['product_id'].apply(str_to_int)

    df = df[col_names]
    print(df.info())

    # Airflow Connection을 통해 DB 연결
    hook = PostgresHook(postgres_conn_id='postgres_naver_conn')
    engine = hook.get_sqlalchemy_engine()

    # 테이블 생성
    with engine.connect() as conn:
        trans = conn.begin()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS naver_shopping(
                id text PRIMARY KEY,
                dt timestamp,
                keyword_type text,
                keyword text,
                brand text,
                category1 text,
                category2 text,
                category3 text,
                category4 text,
                hprice bigint,
                image text,
                link text,
                lprice bigint,
                maker text,
                mall_name text,
                product_id bigint,
                product_type text,
                title text
            )
        """)
        trans.commit()

    # 임시 테이블로 적재
    df.to_sql('naver_shopping_temp', con=engine, if_exists='replace', index=False)

    # 중복 제거 및 삽입
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            conn.execute("""
                DELETE FROM naver_shopping
                USING naver_shopping_temp
                WHERE naver_shopping.id = naver_shopping_temp.id;
            """)
            conn.execute("""
                INSERT INTO naver_shopping
                SELECT * FROM naver_shopping_temp;
            """)
            conn.execute("DROP TABLE IF EXISTS naver_shopping_temp;")
            trans.commit()
        except Exception as e:
            trans.rollback()
            logging.exception(e)
            raise

    # 로깅
    result = pd.read_sql('SELECT COUNT(*) as count FROM naver_shopping;', con=engine)
    print(f"총 {result['count'].iloc[0]}개 레코드가 저장되었습니다.")

# DAG 정의

with DAG(
    dag_id='naver_shopping_to_postgres_split',
    default_args=default_args,
    schedule_interval='0 * * * *',  # 매 시간 정각
    catchup=False,
    tags=['naver', 'postgres'],
) as dag:

    # 1. 데이터 수집 및 저장
    run_task = load_via_sqlalchemy()

    # 2. anomaly_detection DAG 실행 트리거
    trigger_anomaly_detection = TriggerDagRunOperator(
        task_id='trigger_anomaly_detection',
        trigger_dag_id='anomaly_detection',  # 실행할 대상 DAG ID
        wait_for_completion=False,           # True로 하면 동기 실행됨
        reset_dag_run=True,                  # 기존 실행 중인 DAG 중복 방지
        conf={"source": "naver"},            # 필요시 전달할 파라미터
    )

    run_task >> trigger_anomaly_detection
from datetime import datetime, timedelta
import requests
import pandas as pd
import json
import time
import logging
import re
from pytz import timezone

from airflow import DAG
from airflow.decorators import task
from airflow.hooks.postgres_hook import PostgresHook

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

# Naver API 키는 Variable에서 가져옴
from airflow.models import Variable
NAVER_CLIENT_ID = Variable.get('NAVER_API_CLIENT_ID')
NAVER_CLIENT_SECRET = Variable.get('NAVER_API_CLIENT_SECRET')

# 사용할 컬럼 정의
col_names = [
    'original_id', 'dt', 'keyword_type', 'keyword', 'brand', 'category1',
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

# 데이터 정규화/규격화 함수
def normalize_data(df):
    """데이터 정규화 및 규격화"""
    # 1. 텍스트 데이터 정규화
    text_columns = ['title', 'brand', 'maker', 'mall_name', 'category1', 'category2', 'category3', 'category4']
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace('nan', None)
            df[col] = df[col].replace('', None)
    
    # 2. 가격 데이터 정규화
    df['hprice'] = df['hprice'].apply(str_to_int)
    df['lprice'] = df['lprice'].apply(str_to_int)
    
    # 3. 제품 ID 정규화
    df['product_id'] = df['product_id'].apply(str_to_int)
    
    # 4. URL 정규화
    if 'link' in df.columns:
        df['link'] = df['link'].astype(str).str.strip()
        df['link'] = df['link'].replace('nan', None)
    
    if 'image' in df.columns:
        df['image'] = df['image'].astype(str).str.strip()
        df['image'] = df['image'].replace('nan', None)
    
    # 5. 카테고리 데이터 정규화 (빈 값을 None으로 변경)
    category_cols = ['category1', 'category2', 'category3', 'category4']
    for col in category_cols:
        if col in df.columns:
            df[col] = df[col].replace('', None)
    
    # 6. 제품 타입 정규화
    if 'product_type' in df.columns:
        df['product_type'] = df['product_type'].astype(str).str.strip()
        df['product_type'] = df['product_type'].replace('nan', None)
    
    return df

# 스테이징 테이블 데이터 적재

@task
def load_to_staging(**kwargs):
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        # 1. 키워드 수집
        keyword_dict = {
            '여름': ['제습기', '선풍기', '에어컨', '서큘레이터', '아이스박스'],
            '겨울': ['가습기', '히터', '보온병', '온풍기', '전기매트']
        }
        dfs = []

        for keyword_type, keywords in keyword_dict.items():
            for keyword in keywords:
                try:
                    data = naver_search_shopping(keyword)
                    if 'items' not in data:
                        logger.warning(f"[{keyword}]에 대한 결과 없음.")
                        continue
                    df_keyword = pd.DataFrame(data['items'])
                    df_keyword['keyword_type'] = keyword_type
                    df_keyword['keyword'] = keyword
                    dfs.append(df_keyword)
                    time.sleep(0.5)
                except Exception as e:
                    logger.exception(f"Naver API 요청 실패: {keyword}")
                    continue  # 키워드 하나 실패는 무시

        if not dfs:
            raise Exception("API 요청 실패로 데이터프레임이 없습니다.")

        df = pd.concat(dfs, ignore_index=True)
        df.rename(columns={
            'mallName': 'mall_name',
            'productId': 'product_id',
            'productType': 'product_type'
        }, inplace=True)

        store_dt = kwargs['logical_date'].naive()
        df['dt'] = store_dt
        df['original_id'] = df['product_id'].astype(str) + '_' + store_dt.strftime('%Y-%m-%d_%H')

        df = normalize_data(df)
        df = df[col_names]

        # DB 연결 및 테이블 생성
        hook = PostgresHook(postgres_conn_id='postgres_naver_conn')
        engine = hook.get_sqlalchemy_engine()

        with engine.connect() as conn:
            trans = conn.begin()
            conn.execute("""
                CREATE TABLE IF NOT EXISTS naver_shopping_staging (
                    original_id text PRIMARY KEY,
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
                    title text,
                    created_at timestamp DEFAULT CURRENT_TIMESTAMP
                )
            """)
            trans.commit()

        # TRUNCATE
        with engine.connect() as conn:
            trans = conn.begin()
            try:
                conn.execute("TRUNCATE TABLE naver_shopping_staging;")
                trans.commit()
                logger.info("스테이징 테이블 초기화 완료")
            except Exception as e:
                trans.rollback()
                logger.exception("TRUNCATE 실패")
                raise

        # INSERT
        df.to_sql('naver_shopping_staging', con=engine, if_exists='append', index=False)
        result = pd.read_sql('SELECT COUNT(*) AS count FROM naver_shopping_staging;', con=engine)
        logger.info(f"스테이징 테이블에 총 {result['count'].iloc[0]}개 레코드 저장됨")

    except Exception as e:
        logger.exception("load_to_staging 태스크에서 치명적 오류 발생")
        raise  # 반드시 raise 해야 Airflow가 Task를 실패로 인식

# DAG 정의
with DAG(
    dag_id='naver_shopping_st_dag',
    default_args=default_args,
    schedule_interval='0 * * * *',  # 매 시간 정각
    catchup=False,
    tags=['naver', 'staging', 'st'],
    description='네이버 쇼핑 데이터 스테이징 DAG - 데이터 수집 및 정규화'
) as dag:

    staging_task = load_to_staging
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

# Naver API 정보
NAVER_CLIENT_ID = Variable.get('NAVER_API_CLIENT_ID')
NAVER_CLIENT_SECRET = Variable.get('NAVER_API_CLIENT_SECRET')

col_names = [
    'id',
    'dt',
    'keyword_type',
    'keyword',
    'brand',
    'category1',
    'category2',
    'category3',
    'category4',
    'hprice',
    'image',
    'link',
    'lprice',
    'maker',
    'mall_name',
    'product_id',
    'product_type',
    'title'
]


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
        'exclude': 'used:cbshop:rental'  # 중고, 해외구매, 렌탈 제외

    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        result = response.json()
        print(f"조회 성공:{query}")
        return result
    else:
        print(f"요청 실패: {response.status_code}")
        print(response.text)
        raise


def str_to_int(s: str):
    s = s.strip()

    return int(s) if s else 0


@task
def load_via_sqlalchemy(**kwargs):
    keyword_dict = {
        '여름': ['제습기', '선풍기', '에어컨', '서큘레이터', '아이스박스'],
        '겨울': ['가습기', '히터', '보온병', '온풍기', '전기매트']
    }
    dfs = []

    for keyword_type, keywords in keyword_dict.items():
        for keyword in keywords:
            df_keword = pd.DataFrame(naver_search_shopping(keyword)['items'])
            df_keword['keyword_type'] = keyword_type
            df_keword['keyword'] = keyword

            dfs.append(df_keword)

    df = pd.concat(dfs)
    df.rename(columns={'mallName': 'mall_name', 'productId': 'product_id', 'productType': 'product_type'},
              inplace=True)

    store_dt = kwargs['logical_date'].naive()
    df['dt'] = store_dt
    df['id'] = df['product_id'] + '_' + store_dt.strftime('%Y-%m-%d_%H')

    df['hprice'] = df['hprice'].apply(str_to_int)
    df['lprice'] = df['lprice'].apply(str_to_int)
    df['product_id'] = df['product_id'].apply(str_to_int)

    df = df[col_names]
    print(df.info())

    # 반복문으로 추가로 다른 키워드를 api로조회
    # 컬럼 추가 및 전처리

    engine = create_engine(Variable.get('POSTGRE_NAVER_CONN'))
    with engine.connect() as conn:
        trans = conn.begin()  # 트랜잭션 시작
        conn.execute("""
            CREATE TABLE IF NOT EXISTS naver_price(
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

    # 1. 임시 테이블에 적재
    df.to_sql('naver_price_temp', con=engine, if_exists='replace', index=False)  # 우선 public schema이용

    # 중복되는 값 제거하고 insert
    with engine.connect() as conn:
        trans = conn.begin()  # 트랜잭션 시작

        try:
            conn.execute("""
                DELETE FROM naver_price
                USING naver_price_temp
                WHERE naver_price.id = naver_price_temp.id;
            """)

            conn.execute("""
                INSERT INTO naver_price
                SELECT * FROM naver_price_temp;
            """)

            conn.execute("""
                DROP TABLE IF EXISTS naver_price_temp;
            """)
            trans.commit()  # 커밋

        except Exception as e:
            trans.rollback()  # 실패 시 롤백
            logging.exception(e)
            raise 

    # 삽입확인 나중에 삭제
    result = pd.read_sql('SELECT * FROM naver_price;', con=engine)
    print(result)
    print(result.info())


with DAG(
        dag_id='naver_api_to_postgres',
        default_args=default_args,
        schedule_interval='0 * * * *', #hourly
        catchup=False,
        tags=['naver', 'postgres'],
) as dag:
    # DAG 실행 순서 정의
    load_via_sqlalchemy()

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
from pytz import timezone
import pandas as pd
import requests
import json
import logging
import time

KST = timezone('Asia/Seoul')

default_args = {
    'start_date': datetime(2025, 6, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

def load_keywords(**context):
    try:
        keyword_map = json.loads(Variable.get("NAVER_KEYWORD_MAP"))
        context['ti'].xcom_push(key='keyword_map', value=keyword_map)
        logging.info(f"✅ 키워드 로딩 성공: 총 {len(keyword_map)}개 키워드")
    except Exception as e:
        logging.error(f"❌ 키워드 로딩 실패: {e}")
        raise


def collect_data(**context):
    keyword_map = context['ti'].xcom_pull(key='keyword_map', task_ids='load_keywords')
    keywords = list(keyword_map.keys())
    all_items = []

    logging.info(f"🔍 API 호출 대상 키워드 수: {len(keywords)}")

    for keyword in keywords:
        result = None
        for attempt in range(3):
            result = naver_search_shopping(keyword)
            if result:
                break
            logging.warning(f"⚠ API 실패, 재시도 {attempt + 1}/3: {keyword}")
            time.sleep(2)
        if not result:
            logging.error(f"❌ 최종 API 실패: {keyword}")
            continue

        now = datetime.now(KST)
        for item in result.get('items', []):
            item['keyword'] = keyword
            item['dt'] = now
            item['keyword_type'] = keyword_map[keyword]
            all_items.append(item)

    df = pd.DataFrame(all_items)

    if df.empty:
        logging.warning("⚠ 수집된 데이터 없음")
    else:
        logging.info(f"✅ 수집 완료: {len(df)}건")
        logging.info(f"📝 예시:\n{df.head(3).to_string(index=False)}")

    context['ti'].xcom_push(key='collected_df', value=df.to_json(orient='records', date_format='iso'))


def insert_to_postgres(**context):
    df_json = context['ti'].xcom_pull(key='collected_df', task_ids='collect_data')
    df = pd.read_json(df_json)

    if df.empty:
        logging.warning("⛔ 적재할 데이터가 없습니다.")
        return

    logging.info(f"📥 적재 대상 레코드 수: {len(df)}")

    hook = PostgresHook(postgres_conn_id='my_postgres_conn_id')
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.naver_price (
            dt TIMESTAMP,
            keyword_type TEXT,
            keyword TEXT,
            brand TEXT,
            category1 TEXT,
            category2 TEXT,
            category3 TEXT,
            category4 TEXT,
            hprice BIGINT,
            image TEXT,
            link TEXT,
            lprice BIGINT,
            maker TEXT,
            mallName TEXT,
            productId TEXT,
            productType INT,
            title TEXT
        );
    """)

    cur.execute("TRUNCATE TABLE public.naver_price;")
    logging.info("🧹 기존 데이터 TRUNCATE 완료")

    success_count = 0
    for _, row in df.iterrows():
        try:
            cur.execute("""
                INSERT INTO public.naver_price (
                    dt, keyword_type, keyword,
                    brand, category1, category2, category3, category4,
                    hprice, image, link, lprice,
                    maker, mallName, productId, productType, title
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                row.get('dt'),
                row.get('keyword_type'),
                row.get('keyword'),
                row.get('brand'),
                row.get('category1'),
                row.get('category2'),
                row.get('category3'),
                row.get('category4'),
                int(row.get('hprice', 0)) if row.get('hprice') else 0,
                row.get('image'),
                row.get('link'),
                int(row.get('lprice', 0)) if row.get('lprice') else 0,
                row.get('maker'),
                row.get('mallName'),
                row.get('productId'),
                row.get('productType'),
                row.get('title'),
            ))
            success_count += 1
        except Exception as e:
            logging.error(f"❗ INSERT 실패: {e}")

    conn.commit()
    cur.close()
    conn.close()

    logging.info(f"✅ 총 {success_count}건 데이터 적재 완료")


def naver_search_shopping(query, display=100):
    url = "https://openapi.naver.com/v1/search/shop.json"
    headers = {
        "X-Naver-Client-Id": Variable.get("NAVER_CLIENT_ID"),
        "X-Naver-Client-Secret": Variable.get("NAVER_CLIENT_SECRET"),
    }
    params = {
        'query': query,
        'display': display,
        'sort': 'sim',
        'exclude': 'used:cbshop:rental'
    }
    response = requests.get(url, headers=headers, params=params)
    if response.status_code != 200:
        logging.warning(f"⚠ API 응답 실패({response.status_code}): {query}")
        return None

    return response.json()


with DAG(
    dag_id='st_dag',
    default_args=default_args,
    schedule_interval='@hourly',
    tags=['naver', 'shopping', 'postgres'],
    catchup=False
) as dag:
    t1 = PythonOperator(
        task_id='load_keywords',
        python_callable=load_keywords
    )

    t2 = PythonOperator(
        task_id='collect_data',
        python_callable=collect_data
    )

    t3 = PythonOperator(
        task_id='insert_to_postgres',
        python_callable=insert_to_postgres
    )

    t1 >> t2 >> t3

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
import requests
import json
import email.utils as eut
import logging
import psycopg2

# 로깅 기본 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ✅ 데이터 수집 함수
def fetch_shop_data(**context):
    try:
        keyword_map = json.loads(Variable.get("NAVER_KEYWORD_MAP"))
    except Exception as e:
        logger.error(f"❌ NAVER_KEYWORD_MAP Variable 로딩 실패: {e}")
        return

    keywords = list(keyword_map.keys())
    headers = {
        "X-Naver-Client-Id": Variable.get("NAVER_CLIENT_ID"),
        "X-Naver-Client-Secret": Variable.get("NAVER_CLIENT_SECRET")
    }

    now = datetime.now()
    dt_str = now.strftime('%Y-%m-%d %H:%M')
    dt_hour_str = now.strftime('%Y-%m-%d_%H-%M')

    result_list = []

    for kw in keywords:
        try:
            url = f"https://openapi.naver.com/v1/search/shop.json?query={kw}&display=100"
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                data = response.json()
                items = data.get("items", [])
                for item in items:
                    result_list.append({
                        "id": f"{item.get('productId')}_{dt_hour_str}",
                        "dt": dt_str,
                        "keyword_type": keyword_map.get(kw, "알수없음"),
                        "keyword": kw,
                        "brand": item.get("brand"),
                        "category1": item.get("category1"),
                        "category2": item.get("category2"),
                        "category3": item.get("category3"),
                        "category4": item.get("category4"),
                        "hprice": int(item.get("hprice") or 0),
                        "image": item.get("image"),
                        "link": item.get("link"),
                        "lprice": int(item.get("lprice") or 0),
                        "maker": item.get("maker"),
                        "mall_name": item.get("mallName"),
                        "product_id": int(item.get("productId")),
                        "product_type": item.get("productType"),
                        "title": item.get("title")
                    })
            else:
                logger.warning(f"❌ {kw} 검색 실패 - status: {response.status_code}")
        except Exception as e:
            logger.error(f"❌ {kw} 처리 중 예외 발생: {e}", exc_info=True)

    logger.info(f"✅ 총 {len(result_list)}개 수집 완료")
    context['ti'].xcom_push(key='shop_data', value=result_list)

# ✅ PostgreSQL 저장 함수
def save_to_postgres(**context):
    data_list = context['ti'].xcom_pull(
        key='shop_data',
        task_ids='fetch_shop_data'
    )

    if not data_list:
        logger.warning("⚠️ 저장할 데이터가 없습니다.")
        return

    try:
        conn = psycopg2.connect(
            host="postgres-naver",
            port="5432",
            dbname="naver",
            user="naver",
            password="naver"
        )
        cur = conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS shop_data (
            id TEXT PRIMARY KEY,
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
            mall_name TEXT,
            product_id BIGINT,
            product_type TEXT,
            title TEXT
        );
        """)

        insert_sql = """
        INSERT INTO shop_data (
            id, dt, keyword_type, keyword, brand,
            category1, category2, category3, category4,
            hprice, image, link, lprice, maker,
            mall_name, product_id, product_type, title
        ) VALUES (
            %(id)s, %(dt)s, %(keyword_type)s, %(keyword)s, %(brand)s,
            %(category1)s, %(category2)s, %(category3)s, %(category4)s,
            %(hprice)s, %(image)s, %(link)s, %(lprice)s, %(maker)s,
            %(mall_name)s, %(product_id)s, %(product_type)s, %(title)s
        ) ON CONFLICT (id) DO NOTHING;
        """

        for row in data_list:
            try:
                cur.execute(insert_sql, row)
            except Exception as e:
                logger.error(f"❌ 행 삽입 실패: {row['id']} / 에러: {e}")

        conn.commit()
        logger.info(f"✅ 총 {len(data_list)}개 항목 저장 완료")

    except Exception as e:
        logger.error(f"❌ DB 연결 또는 삽입 실패: {e}", exc_info=True)
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

# ✅ DAG 정의
default_args = {
    'owner': 'emati',
    'start_date': datetime(2025, 6, 9),
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

with DAG(
    dag_id='naver_shop_etl',
    default_args=default_args,
    schedule_interval='0 * * * *',  # 매 정시마다 실행
    catchup=False
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_shop_data',
        python_callable=fetch_shop_data,
        provide_context=True
    )

    save_task = PythonOperator(
        task_id='save_to_postgres',
        python_callable=save_to_postgres,
        provide_context=True
    )

    fetch_task >> save_task

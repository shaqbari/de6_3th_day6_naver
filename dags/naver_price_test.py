
from datetime import datetime
import requests
import json
import email.utils as eut

def fetch_shop_data(**context):
    summer_keywords = ["제습기", "선풍기", "에어컨", "서큘레이터", "아이스박스"]
    winter_keywords = ["가습기", "히터", "보온병", "온풍기", "전기매트"]
    
    keyword_type = context['params'].get('keyword_type', '여름')
    keywords = summer_keywords if keyword_type == '여름' else winter_keywords

    headers = {
        "X-Naver-Client-Id": "CcZE1APHSuU2EmHQcGLI",
        "X-Naver-Client-Secret": "6YtebxJkOd"
    }

    result_list = []

    for kw in keywords:
        url = f"https://openapi.naver.com/v1/search/shop.json?query={kw}&display=100"
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            build_date = data.get("lastBuildDate")
            parsed_dt = eut.parsedate_to_datetime(build_date)
            dt_str = parsed_dt.strftime('%Y-%m-%d %H:%M')
            dt_hour_str = parsed_dt.strftime('%Y-%m-%d_%H')

            items = data.get("items", [])
            for item in items:
                result_list.append({
                    "id": f"{item.get('productId')}_{dt_hour_str}",
                    "dt": dt_str,
                    "keyword_type": keyword_type,
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
            print(f" {kw} 검색 실패 - status: {response.status_code}")

    context['ti'].xcom_push(key='shop_data', value=result_list)
import psycopg2

def save_to_postgres(**context):
    data_list = context['ti'].xcom_pull(
        key='shop_data', 
        task_ids='fetch_shop_data'  # Task1의 task_id 정확히 입력
    )

    if not data_list:
        print("저장할 데이터가 없습니다.")
        return

    conn = psycopg2.connect(
        host="postgres",     # docker-compose 내 서비스 이름
        dbname="airflow",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

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
            print(f"행 삽입 실패: {row['id']} / 에러: {e}")

    conn.commit()
    cur.close()
    conn.close()
    print(f"총 {len(data_list)}개 항목 저장 완료")


from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'emati',
    'start_date': datetime(2024, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

with DAG(
    dag_id='naver_shop_etl',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_shop_data',
        python_callable=fetch_shop_data,
        provide_context=True,
        params={'keyword_type': '여름'}  # 또는 '겨울'
    )

    save_task = PythonOperator(
        task_id='save_to_postgres',
        python_callable=save_to_postgres,
        provide_context=True
    )

    fetch_task >> save_task

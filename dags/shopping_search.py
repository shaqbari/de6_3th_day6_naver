from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import psycopg2
import requests
import logging
import email.utils as eut
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# 키워드 리스트
summer_keywords = ["제습기", "선풍기", "에어컨", "서큘레이터", "아이스박스"]
winter_keywords = ["가습기", "히터", "보온병", "온풍기", "전기매트"]

# 키워드 계절 분류 함수
def get_keyword_type(keyword):
    if keyword in summer_keywords:
        return "여름"
    elif keyword in winter_keywords:
        return "겨울"
    return "기타"

# DAG 기본 설정
default_args = {
    'owner': 'yeanjun',
    'start_date': datetime(2025, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

# DAG 정의
with DAG(
    dag_id='shopping_search_v1',
    default_args=default_args,
    schedule_interval='0 */2 * * *',
    catchup=False,
    tags=['naver', 'shop', 'ETL']
) as dag:

    @task
    def extract_transform():
        logging.info("네이버 쇼핑 API 연결 시작")
        headers = {
            "X-Naver-Client-Id": "6TO9yUZ5i9279Nduq107",
            "X-Naver-Client-Secret": "2NF5yZ34gW",
        }

        results = []
        all_keywords = summer_keywords + winter_keywords

        for word in all_keywords:
            params = {
                'query': word,
                'display': 100,
                'sort': 'sim',
                'exclude': 'used:cbshop:rental'
            }
            response = requests.get("https://openapi.naver.com/v1/search/shop.json", headers=headers, params=params)
            if response.status_code == 200:
                logging.info(f"{word} 검색 성공")
                data = response.json()
                lastBuildDate = data.get("lastBuildDate")
                parsed_dt = eut.parsedate_to_datetime(lastBuildDate)
                dt_str = parsed_dt.strftime('%Y-%m-%d %H:%M')
                dt_hour_str = parsed_dt.strftime('%Y-%m-%d_%H')

                items = data.get("items", [])
                for item in items:
                    results.append({
                        "id": f"{item.get('productId')}_{dt_hour_str}",
                        "dt": dt_str,
                        "keyword_type": get_keyword_type(word),
                        "keyword": word,
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
                logging.warning(f"{word} 검색 실패 - status: {response.status_code}")

        logging.info("데이터 추출 완료")
        return results

    @task
    def load_to_postgres(data_list):
        if not data_list:
            logging.warning("저장할 데이터가 없습니다.")
            return

        try:
            conn = psycopg2.connect(
                host="host.docker.internal",
                port=5433,
                dbname="naver",
                user="naver",
                password="naver",
                options='-c search_path=public'
            )
            cur = conn.cursor()
            logging.info("PostgreSQL 연결 성공")

            insert_sql = """
                INSERT INTO public.shop_data (
                    dt, keyword_type, keyword, brand,
                    category1, category2, category3, category4,
                    hprice, image, link, lprice, maker,
                    mall_name, product_id, product_type, title
                ) VALUES (
                    %(dt)s, %(keyword_type)s, %(keyword)s, %(brand)s,
                    %(category1)s, %(category2)s, %(category3)s, %(category4)s,
                    %(hprice)s, %(image)s, %(link)s, %(lprice)s, %(maker)s,
                    %(mall_name)s, %(product_id)s, %(product_type)s, %(title)s
                )
                ON CONFLICT (id) DO NOTHING;;
            """

            for row in data_list:
                try:
                    cur.execute(insert_sql, row)
                except Exception as e:
                    logging.error(f"행 삽입 실패: {row['id']} / 에러: {e}")
                    conn.rollback()

            conn.commit()
            logging.info(f"총 {len(data_list)}개 항목 저장 완료")
            logging.info(f"첫 번째 행 확인: {data_list[0] if data_list else '비어 있음'}")

        except Exception as e:
            logging.error(f"PostgreSQL 연결 또는 저장 중 오류 발생: {e}")
        finally:
            if 'cur' in locals():
                cur.close()
            if 'conn' in locals():
                conn.close()

    # DAG 워크플로우 연결
    data = extract_transform()
    load = load_to_postgres(data)

    trigger_summary_dag = TriggerDagRunOperator(
        task_id='trigger_summary_dbt_dag',
        trigger_dag_id='summary_dbt_dag',  # 실행 대상 DAG ID
        wait_for_completion=False,
        reset_dag_run=True,
        conf={"triggered_by": "shopping_search_v1"}  # 선택 사항
    )

    # 흐름 연결
    load >> trigger_summary_dag
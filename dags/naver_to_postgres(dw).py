from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import logging

default_args = {
    'start_date': datetime(2025, 6, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

def transfer_to_dw(**context):
    hook = PostgresHook(postgres_conn_id='my_postgres_conn_id')
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        logging.info("🛠 DW 테이블 생성 시도")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.dw_naver_price (
                id SERIAL PRIMARY KEY,
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
                mallname TEXT,
                productid TEXT,
                producttype INT,
                title TEXT,
                inserted_at TIMESTAMP DEFAULT NOW()
            );
        """)
        conn.commit()
        logging.info("✅ DW 테이블 생성 완료 또는 이미 존재")

        # ✅ ST 테이블에서 데이터 조회
        cur.execute("SELECT * FROM public.naver_price;")
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        df_st = pd.DataFrame(rows, columns=colnames)

        logging.info(f"📋 df_st.columns: {df_st.columns.tolist()}")
        logging.info(f"\n🧾 df_st.head():\n{df_st.head().to_string(index=False)}")

        if df_st.empty:
            logging.warning("⛔ ST 테이블에서 불러온 데이터가 없습니다.")
            return

        logging.info(f"📦 ST 테이블에서 {len(df_st)}건 데이터 조회")

        inserted_count = 0
        for _, row in df_st.iterrows():
            cur.execute("""
                INSERT INTO public.dw_naver_price (
                    dt, keyword_type, keyword,
                    brand, category1, category2, category3, category4,
                    hprice, image, link, lprice,
                    maker, mallname, productid, producttype, title
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                row['dt'],
                row['keyword_type'],
                row['keyword'],
                row['brand'],
                row['category1'],
                row['category2'],
                row['category3'],
                row['category4'],
                int(row['hprice']) if pd.notnull(row['hprice']) else 0,
                row['image'],
                row['link'],
                int(row['lprice']) if pd.notnull(row['lprice']) else 0,
                row['maker'],
                row['mallname'],
                row['productid'],
                int(row['producttype']) if pd.notnull(row['producttype']) else 0,
                row['title'],
            ))
            inserted_count += 1

        conn.commit()

        cur.execute("SELECT COUNT(*) FROM public.dw_naver_price;")
        total_count = cur.fetchone()[0]

        logging.info(f"✅ DW에 총 {inserted_count}건 적재 완료")
        logging.info(f"📊 DW 전체 누적 건수: {total_count}")

    except Exception as e:
        conn.rollback()
        logging.error(f"❌ 전체 적재 중 오류 발생. 롤백됨: {e}")
        raise e

    finally:
        cur.close()
        conn.close()

with DAG(
    dag_id='dw_dag',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
    tags=['dw', 'naver', 'postgres']
) as dag:

    t1 = ExternalTaskSensor(
        task_id='wait_for_st_dag',
        external_dag_id='st_dag',
        external_task_id='insert_to_postgres',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        execution_delta=timedelta(hours=0),
        mode='reschedule',
        poke_interval=60,
        timeout=600
    )

    t2 = PythonOperator(
        task_id='transfer_to_dw',
        python_callable=transfer_to_dw
    )

    t1 >> t2

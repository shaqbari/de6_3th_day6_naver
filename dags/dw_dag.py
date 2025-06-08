from datetime import datetime, timedelta
import logging
import pandas as pd
from pytz import timezone

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

@task
def load_to_datamart(**kwargs):
    """스테이징 테이블에서 데이터마트로 데이터 적재"""
    logger = logging.getLogger(__name__)
    
    try:
        # Airflow 2.6+의 context에서 logical date는 data_interval_start
        logical_date = kwargs['data_interval_start']
        KST = timezone('Asia/Seoul')
        current_time = logical_date.astimezone(KST).replace(tzinfo=None)
        logger.info(f"처리할 논리적 날짜: {logical_date}")

        hook = PostgresHook(postgres_conn_id='postgres_naver_conn')
        engine = hook.get_sqlalchemy_engine()

        with engine.begin() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS naver_shopping_datamart(
                    id SERIAL PRIMARY KEY,
                    original_id text,
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
                    etl_timestamp timestamp DEFAULT CURRENT_TIMESTAMP,
                    created_at timestamp,
                    is_active boolean DEFAULT true,
                    valid_from timestamp DEFAULT CURRENT_TIMESTAMP,
                    valid_to timestamp DEFAULT '9999-12-31 23:59:59'
                )
            """)
            
            index_queries = [
                "CREATE INDEX IF NOT EXISTS idx_naver_datamart_original_id ON naver_shopping_datamart(original_id);",
                "CREATE INDEX IF NOT EXISTS idx_naver_datamart_product_id ON naver_shopping_datamart(product_id);",
                "CREATE INDEX IF NOT EXISTS idx_naver_datamart_etl_timestamp ON naver_shopping_datamart(etl_timestamp);",
                "CREATE INDEX IF NOT EXISTS idx_naver_datamart_is_active ON naver_shopping_datamart(is_active);"
            ]
            for query in index_queries:
                conn.execute(query)
            
            logger.info("데이터마트 테이블 및 인덱스 생성 완료")

        with engine.connect() as conn:
            staging_data = pd.read_sql("SELECT * FROM naver_shopping_staging ORDER BY original_id", con=conn)

            if staging_data.empty:
                logger.info("스테이징 테이블에 데이터가 없습니다.")
                return

            logger.info(f"스테이징 테이블에서 {len(staging_data)}개 레코드 조회")

            processed_count = 0
            new_count = 0
            updated_count = 0
            unchanged_count = 0

            with conn.begin():
                for _, row in staging_data.iterrows():
                    try:
                        existing_query = """
                            SELECT id, hprice, lprice, title, brand, maker, mall_name 
                            FROM naver_shopping_datamart 
                            WHERE original_id = %s AND is_active = true
                            ORDER BY etl_timestamp DESC
                            LIMIT 1
                        """
                        existing_record = pd.read_sql(existing_query, con=conn, params=[row['original_id']])

                        def safe_compare(val1, val2):
                            if pd.isna(val1) and pd.isna(val2):
                                return False
                            if pd.isna(val1) or pd.isna(val2):
                                return True
                            return val1 != val2

                        if not existing_record.empty:
                            existing = existing_record.iloc[0]
                            has_changes = any([
                                safe_compare(existing['hprice'], row['hprice']),
                                safe_compare(existing['lprice'], row['lprice']),
                                safe_compare(existing['title'], row['title']),
                                safe_compare(existing['brand'], row['brand']),
                                safe_compare(existing['maker'], row['maker']),
                                safe_compare(existing['mall_name'], row['mall_name'])
                            ])

                            if has_changes:
                                conn.execute("""
                                    UPDATE naver_shopping_datamart 
                                    SET is_active = false, valid_to = %s
                                    WHERE id = %s
                                """, [current_time, existing['id']])

                                insert_query = """
                                    INSERT INTO naver_shopping_datamart 
                                    (original_id, dt, keyword_type, keyword, brand, 
                                    category1, category2, category3, category4, 
                                    hprice, image, link, lprice, maker, mall_name, 
                                    product_id, product_type, title, etl_timestamp, created_at, valid_from)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                """
                                values = [
                                    row['original_id'], row['dt'], row['keyword_type'], row['keyword'],
                                    row.get('brand'), row.get('category1'), row.get('category2'), row.get('category3'),
                                    row.get('category4'), row['hprice'], row.get('image'), row.get('link'),
                                    row['lprice'], row.get('maker'), row.get('mall_name'), row['product_id'],
                                    row.get('product_type'), row.get('title'), current_time,
                                    row.get('created_at', current_time), current_time
                                ]
                                conn.execute(insert_query, values)
                                updated_count += 1
                            else:
                                conn.execute("""
                                    UPDATE naver_shopping_datamart 
                                    SET etl_timestamp = %s
                                    WHERE id = %s
                                """, [current_time, existing['id']])
                                unchanged_count += 1
                        else:
                            insert_query = """
                                INSERT INTO naver_shopping_datamart 
                                (original_id, dt, keyword_type, keyword, brand, 
                                category1, category2, category3, category4, 
                                hprice, image, link, lprice, maker, mall_name, 
                                product_id, product_type, title, etl_timestamp, created_at, valid_from)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                            values = [
                                row['original_id'], row['dt'], row['keyword_type'], row['keyword'],
                                row.get('brand'), row.get('category1'), row.get('category2'), row.get('category3'),
                                row.get('category4'), row['hprice'], row.get('image'), row.get('link'),
                                row['lprice'], row.get('maker'), row.get('mall_name'), row['product_id'],
                                row.get('product_type'), row.get('title'), current_time,
                                row.get('created_at', current_time), current_time
                            ]
                            conn.execute(insert_query, values)
                            new_count += 1

                        processed_count += 1

                    except Exception as e:
                        logger.error(f"레코드 처리 중 오류 발생: {row['original_id']}, 오류: {str(e)}")
                        continue

            logger.info(f"처리 완료 - 총: {processed_count}, 신규: {new_count}, 업데이트: {updated_count}, 변경없음: {unchanged_count}")

        with engine.connect() as conn:
            stats = pd.read_sql("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN is_active = true THEN 1 END) as active_records,
                    COUNT(CASE WHEN is_active = false THEN 1 END) as inactive_records,
                    MAX(etl_timestamp) as last_etl,
                    COUNT(DISTINCT original_id) as unique_products
                FROM naver_shopping_datamart
            """, con=conn)

            logger.info("=== ✅ 데이터마트 통계 ===")
            logger.info(f"총 레코드 수: {stats['total_records'].iloc[0]}")
            logger.info(f"활성 레코드 수: {stats['active_records'].iloc[0]}")
            logger.info(f"비활성 레코드 수: {stats['inactive_records'].iloc[0]}")
            logger.info(f"고유 상품 수: {stats['unique_products'].iloc[0]}")
            logger.info(f"마지막 ETL 시간: {stats['last_etl'].iloc[0]}")

    except Exception as e:
        logger.exception("load_to_datamart 전체 실행 중 예외 발생")
        raise

@task
def validate_data_quality():
    logger = logging.getLogger(__name__)
    
    try:
        hook = PostgresHook(postgres_conn_id='postgres_naver_conn')
        engine = hook.get_sqlalchemy_engine()
        quality_checks = []

        with engine.connect() as conn:
            duplicates = pd.read_sql("""
                SELECT original_id, COUNT(*) as cnt
                FROM naver_shopping_datamart
                WHERE is_active = true
                GROUP BY original_id
                HAVING COUNT(*) > 1
            """, con=conn)
            if not duplicates.empty:
                quality_checks.append(f"중복 활성 레코드 발견: {len(duplicates)}개")

            nulls = pd.read_sql("""
                SELECT COUNT(*) as null_count
                FROM naver_shopping_datamart
                WHERE is_active = true
                AND (original_id IS NULL OR keyword IS NULL OR keyword_type IS NULL)
            """, con=conn)
            if nulls['null_count'].iloc[0] > 0:
                quality_checks.append(f"필수 필드 NULL 레코드: {nulls['null_count'].iloc[0]}개")

            prices = pd.read_sql("""
                SELECT COUNT(*) as invalid_price_count
                FROM naver_shopping_datamart
                WHERE is_active = true
                AND (hprice < 0 OR lprice < 0 OR (hprice > 0 AND lprice > 0 AND hprice < lprice))
            """, con=conn)
            if prices['invalid_price_count'].iloc[0] > 0:
                quality_checks.append(f"잘못된 가격 데이터: {prices['invalid_price_count'].iloc[0]}개")

        if quality_checks:
            logger.warning("=== ⚠️ 데이터 품질 이슈 ===")
            for issue in quality_checks:
                logger.warning(f"❌ {issue}")
        else:
            logger.info("✅ 데이터 품질 검증 통과")

    except Exception as e:
        logger.exception("데이터 품질 검증 중 오류 발생")
        raise

with DAG(
    dag_id='naver_shopping_dw_dag',
    default_args=default_args,
    schedule_interval='15 * * * *',
    catchup=False,
    tags=['naver', 'data_warehouse', 'dw'],
    description='네이버 쇼핑 데이터 DW 적재 DAG - 스테이징 이후 데이터 적재',
) as dag:

    wait_staging = ExternalTaskSensor(
        task_id='wait_for_staging',
        external_dag_id='naver_shopping_st_dag',
        external_task_id='load_to_staging',
        allowed_states=['success'],
        failed_states=['failed', 'upstream_failed', 'skipped'],
        timeout=600,
        poke_interval=30,
        mode='poke'
    )

    load_dw = load_to_datamart()
    quality_check = validate_data_quality()

    wait_staging >> load_dw >> quality_check

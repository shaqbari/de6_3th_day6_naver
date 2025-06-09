from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

@task
def detect_and_save_summary():
    # PostgreSQL 연결
    hook = PostgresHook(postgres_conn_id='postgres_naver_conn')
    engine = hook.get_sqlalchemy_engine()

    # 전체 데이터 로딩
    df = pd.read_sql("SELECT * FROM naver_shopping", con=engine)

    # datetime 처리
    df['dt'] = pd.to_datetime(df['dt'])
    df['hour'] = df['dt'].dt.floor('h')

    # 전체 키워드 대상 (10개 모두)
    df_filtered = df[df['keyword'].notnull()]

    # 요약 집계
    result = df_filtered.groupby(['hour', 'keyword'])['lprice'].agg(
        avg_price='mean',
        min_price='min',
        max_price='max'
    ).reset_index()

    # 평균 정수화 및 id 생성
    result['avg_price'] = result['avg_price'].astype(int)
    result['id'] = result['hour'].astype(str)

    # 컬럼 순서 정리
    result = result[['id', 'keyword', 'avg_price', 'min_price', 'max_price']]

    # 테이블 생성 (최초 1회만 작동)
    with engine.connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS summary_shop_keyword (
                id text PRIMARY KEY,
                keyword text,
                avg_price bigint,
                min_price bigint,
                max_price bigint
            );
        """)

    # 기존 중복 제거 후 insert
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            if not result.empty:
                result_ids = tuple(result['id'].tolist())
                if len(result_ids) == 1:
                    conn.execute(
                        "DELETE FROM summary_shop_keyword WHERE id = :id",
                        {"id": result_ids[0]}
                    )
                else:
                    # 튜플 형태로 문자열을 직접 만들어서 쿼리에 넣기
                    ids_str = ','.join(f"'{i}'" for i in result_ids)
                    conn.execute(f"DELETE FROM summary_shop_keyword WHERE id IN ({ids_str})")
                
                result.to_sql('summary_shop_keyword', con=engine, if_exists='append', index=False)
            trans.commit()
        except Exception as e:
            trans.rollback()
            raise


with DAG(
    dag_id='anomaly_detection',
    default_args=default_args,
    schedule_interval=None,  # trigger로 실행
    catchup=False,
    tags=['naver', 'anomaly'],
) as dag:

    detect_and_save_summary()

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


@task
def make_naver_price_summary(**kwargs):
    engine = create_engine(Variable.get('POSTGRE_NAVER_CONN'))
    df_naver_price = pd.read_sql("select * from naver_price", con=engine)

    # 1. 시간 순 정렬 (상품 단위)
    df_sorted = df_naver_price.sort_values(by=['product_id', 'dt'])

    # 2. 전 시간대 가격 추가
    df_sorted['prev_lprice'] = df_sorted.groupby('product_id')['lprice'].shift(1)

    # 3. 변화율 계산 ((현재 - 이전) / 이전)
    df_sorted['price_change_rate'] = (df_sorted['lprice'] - df_sorted['prev_lprice']) / df_sorted['prev_lprice']

    # 4. 변화율(%) 보기 쉽게 포맷팅 (선택사항)
    df_sorted['price_change_rate_pct'] = df_sorted['price_change_rate'] * 100

    # 5. 필요시 NaN 제거 (첫 행은 이전 값이 없으므로)
    df_result = df_sorted.dropna(subset=['price_change_rate'])

    # 결과 확인
    print(df_result[['product_id', 'dt', 'lprice', 'prev_lprice', 'price_change_rate_pct']])

    # product_id 별 분석 시작
    # 1. 최근 24시간 가격 데이터 준비
    latest_dt = df_result['dt'].max()
    time_threshold = latest_dt - pd.Timedelta(hours=24)
    df_recent = df_result[df_result['dt'] > time_threshold]

    # 2. product_id, dt 순 정렬
    df_recent = df_recent.sort_values(by=['product_id', 'dt'])

    # 3. 분석 결과 저장 리스트
    buy_signals = []

    # 4. product_id 별로 분석
    for product_id, group in df_recent.groupby('product_id'):
        lprices = group['lprice'].values
        times = group['dt'].values

        if len(lprices) < 12:
            continue  # 데이터가 너무 적으면 스킵

        # 이동 평균 (최근 12시간 기준)
        ma12 = pd.Series(lprices).rolling(window=12).mean().values

        # 시작 가격
        first_price = lprices[0]
        # 현재 가격 (가장 최근)
        last_price = lprices[-1]
        current_time = times[-1]
        current_ma12 = ma12[-1]

        # 24시간 집계
        min_price = lprices.min()
        max_price = lprices.max()
        avg_price = lprices.mean()
        price_diff = (last_price - first_price)
        price_diff_pct = price_diff / first_price * 100

        # 조건: 현재가가 MA3보다 낮고, 최저가에 근접 (5% 이내), price_diff_pct -10이하
        if not pd.isna(
                current_ma12) and last_price < current_ma12 and last_price <= min_price * 1.05 and price_diff_pct <= -10:
            buy_signals.append({
                'product_id': product_id,
                'dt': current_time,
                'first_price': first_price,
                'last_price': last_price,
                'min_price': min_price,
                'max_price': max_price,
                'avg_price': avg_price,
                'price_diff': price_diff,
                'price_diff_pct': price_diff_pct,
            })

    # 결과 DataFrame
    buy_df = pd.DataFrame(buy_signals)
    buy_df.set_index('product_id', inplace=True)

    # 필요한 열만 추출 (중복 제거)
    link_title_info = df_naver_price[['product_id', 'title', 'link', 'keyword', 'keyword_type']].drop_duplicates()

    # product_id를 index로 설정하여 join 준비
    link_title_info.set_index('product_id', inplace=True)

    # filtered_df에 link와 title 병합
    buy_df_with_info = buy_df.join(link_title_info, how='left')
    col_names = ['title', 'dt', 'link', 'keyword', 'keyword_type', 'first_price', 'last_price', 'min_price',
                 'max_price', 'avg_price', 'price_diff', 'price_diff_pct']
    # 결과 확인
    buy_df_with_info = buy_df_with_info[col_names]
    print(buy_df_with_info)
    print(buy_df_with_info.info())


    with engine.connect() as conn:
        trans = conn.begin()  # 트랜잭션 시작
        conn.execute("""
            CREATE TABLE IF NOT EXISTS summary_buy_timing(
                product_id bigint NOT NULL PRIMARY KEY,
                title text,
                dt timestamp,
                link text,
                keyword text,
                keyword_type text,
                first_price bigint,
                last_price bigint,
                min_price bigint,
                max_price bigint,
                avg_price double precision,
                price_diff bigint,
                price_diff_pct double precision
            )
        """)
        trans.commit()

    # 테이블 full refresh
    buy_df_with_info.to_sql('summary_buy_timing', con=engine, if_exists='replace', index=True)

    # 삽입되었는지 확인, 나중에 삭제
    confirm_table = pd.read_sql("SELECT * FROM summary_buy_timing", con=engine)
    print(confirm_table)
    print(confirm_table.info())


with DAG(
        dag_id='seonghyun_summary_buy_timing',
        default_args=default_args,
        schedule_interval='5 * * * *',  # hourly
        catchup=False,
        tags=['naver', 'postgres'],
) as dag:
    # DAG 실행 순서 정의
    make_naver_price_summary()

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

# 키워드 정보
NAVER_KEYWORD_JSON =Variable.get('NAVER_KEYWORD_JSON')

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


def send_slack_message(message: str):
    SLACK_URL = Variable.get("SLACK_WEBHOOK_URL")
    requests.post(SLACK_URL, json={"text": message})



@task
def load_to_postgre(**kwargs):
    keyword_dict = json.loads(NAVER_KEYWORD_JSON)


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
    logging.info(df.head())
    logging.info(df.info())


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
    # result = pd.read_sql('SELECT * FROM naver_price;', con=engine)
    # print(result)
    # print(result.info())



@task
def make_summary_buy_timing(**kwargs):
    engine = create_engine(Variable.get('POSTGRE_NAVER_CONN'))
    df_naver_price = pd.read_sql("select * from naver_price", con=engine)

    df_sorted = df_naver_price.sort_values(by=['product_id', 'dt'])
    df_sorted['prev_lprice'] = df_sorted.groupby('product_id')['lprice'].shift(1)
    df_sorted['price_change_rate'] = (df_sorted['lprice'] - df_sorted['prev_lprice']) / df_sorted['prev_lprice']
    df_sorted['price_change_rate_pct'] = df_sorted['price_change_rate'] * 100
    df_result = df_sorted.dropna(subset=['price_change_rate'])

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

        # 조건: 현재가가 MA12보다 낮고, 최저가에 근접 (5% 이내), price_diff_pct -5이하
        if not pd.isna(
                current_ma12) and last_price < current_ma12 and last_price <= min_price * 1.05 and price_diff_pct <= -5:
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

    if len(buy_signals) == 0:
        logging.warning("구매타이밍의 제품이 없거나 데이터가 12시간이상 수집되지 않음")
        with engine.connect() as conn:
            trans = conn.begin()  # 트랜잭션 시작
            conn.execute("DELETE FROM summary_buy_timing")
            trans.commit()

        return

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
    # confirm_table = pd.read_sql("SELECT * FROM summary_buy_timing", con=engine)
    # print(confirm_table)

@task
def alert_slack_task(**kwargs):
    engine = create_engine(Variable.get('POSTGRE_NAVER_CONN'))

    df = pd.read_sql("SELECT * FROM summary_buy_timing", con=engine)

    if df.empty:
        print("⚠️ 알림 대상 상품 없음.")
        return

    msgs = []
    for _, row in df.iterrows():
        max_drop = -(row['max_price'] - row['last_price'])
        max_drop_pct = -((max_drop / row['max_price']) * 100)

        msg = f"""📢 *[{row['title']}]*  
🔗 <{row['link']}|상품 보러가기>  
🛒 키워드: {row['keyword']} / {row['keyword_type']}  
🕘 분석 기준 시점: {pd.to_datetime(row['dt']).strftime('%Y-%m-%d')}  

💰 최초가: {row['first_price']:,}원 
📉 최저가: {row['min_price']:,}원 
📈 최고가: {row['max_price']:,}원
🧮 평균가: {row['avg_price']:,.2f}원  
💸 현재가: {row['last_price']:,}원  

🔻 최초가 대비: {row['price_diff']:,}원 ({row['price_diff_pct']:.2f}%)  
🔻 최고가 대비: {max_drop:,}원 ({max_drop_pct:.2f}%)
"""
        msgs.append(msg)

    send_slack_message('\n\n'.join(msgs))

with DAG(
        dag_id='seonghyun_etl_elt_alram',
        default_args=default_args,
        schedule_interval='0 * * * *', #hourly
        catchup=False,
        tags=['naver', 'postgres'],
) as dag:
    # DAG 실행 순서 정의
    etl = load_to_postgre()
    elt = make_summary_buy_timing()
    alram = alert_slack_task()

    etl >> elt >> alram

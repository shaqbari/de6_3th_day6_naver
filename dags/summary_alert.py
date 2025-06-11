from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
from sqlalchemy import create_engine
from airflow.models import Variable


# 슬랙 알림 전송 함수
def send_slack_message(message: str):
    SLACK_URL = Variable.get("SLACK_WEBHOOK_URL")
    requests.post(SLACK_URL, json={"text": message})


# summary_buy_timing 기반 알림 전송 함수
def alert_slack_task(**kwargs):
    engine = create_engine(Variable.get("POSTGRE_NAVER_CONN"))

    df = pd.read_sql("""
        SELECT *
        FROM summary_buy_timing
        WHERE price_diff_pct <= -10
        ORDER BY price_diff_pct ASC;
    """, con=engine)

    if df.empty:
        print("⚠️ 알림 대상 상품 없음.")
        return

    for _, row in df.iterrows():
        first_drop = row["first_price"] - row["last_price"]
        max_drop = row["max_price"] - row["last_price"]
        first_drop_pct = (first_drop / row["first_price"]) * 100
        max_drop_pct = (max_drop / row["max_price"]) * 100

        msg = f"""📢 *[{row['title']}]*  
🔗 <{row['link']}|상품 보러가기>  
🛒 키워드: {row['keyword']} / {row['keyword_type']}  
🕘 시점: {row['dt']}  

💰 최초가: {row['first_price']:,}원  
📈 최고가: {row['max_price']:,}원  
📉 최저가: {row['min_price']:,}원  
🧮 평균가: {row['avg_price']:,.2f}원  
📉 현재가: {row['last_price']:,}원  

🔻 최초가 대비 하락: {first_drop:,}원 ({first_drop_pct:.2f}%)  
🔻 최고가 대비 하락: {max_drop:,}원 ({max_drop_pct:.2f}%)
"""
        send_slack_message(msg)


# DAG 정의
with DAG(
    dag_id="send_buy_signal_slack_alert",
    start_date=datetime(2025, 6, 9),
    schedule_interval="10 * * * *",  # 매시 10분 실행
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
    },
    tags=["naver", "slack", "alert"]
) as dag:

    send_alert = PythonOperator(
        task_id="send_summary_buy_alert",
        python_callable=alert_slack_task,
        provide_context=True
    )

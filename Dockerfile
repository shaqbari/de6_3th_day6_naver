# Dockerfile
FROM apache/airflow:2.9.3-python3.12

USER airflow
RUN pip install --no-cache-dir \
    "apache-airflow[cncf.kubernetes,celery,postgres,redis]==2.9.3" \
    dbt-core==1.8.2 \
    dbt-postgres==1.8.2 \
    pandas \
    numpy \
    oauth2client \
    gspread \
    requests \
    pytz

USER airflow
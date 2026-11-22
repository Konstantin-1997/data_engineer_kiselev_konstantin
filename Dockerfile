FROM apache/airflow:2.11.2-python3.11
COPY requirements.txt /requirements.txt
# apache-airflow закреплён: pip не сможет молча поменять версию Airflow ради зависимостей
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

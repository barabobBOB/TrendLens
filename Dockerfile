FROM apache/airflow:2.8.1-python3.11

USER root

COPY requirements.txt ./requirements.txt

USER airflow

RUN pip install -r ./requirements.txt
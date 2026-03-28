ARG AIRFLOW_BASE_IMAGE=apache/airflow:3.1.8-python3.12
FROM ${AIRFLOW_BASE_IMAGE}

USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends git curl openjdk-17-jre-headless \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

COPY infra/ml/requirements.txt /tmp/ml-requirements.txt

RUN pip install --no-cache-dir \
  apache-airflow-providers-apache-spark \
  apache-airflow-providers-openlineage \
  apache-airflow-providers-trino \
  pyspark==3.5.5 \
  spark-nlp==5.5.3 \
  -r /tmp/ml-requirements.txt

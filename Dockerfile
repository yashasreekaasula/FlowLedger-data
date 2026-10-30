# Reproducible educational stack; review/update pins before any deployment.
FROM apache/airflow:2.10.5-python3.11
USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless && apt-get clean && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --no-cache-dir apache-airflow==2.10.5 pyspark==3.5.7 pytest==8.3.5
WORKDIR /opt/flowledger
COPY --chown=airflow:root . /opt/flowledger
ENV PYTHONPATH=/opt/flowledger PROJECT_ROOT=/opt/flowledger SPARK_LOCAL_IP=127.0.0.1

FROM apache/airflow:2.10.0-python3.10

USER root

RUN mkdir -p /tmp && chmod 777 /tmp

USER airflow

COPY --chown=airflow:root requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY --chown=airflow:root dags /opt/airflow/dags
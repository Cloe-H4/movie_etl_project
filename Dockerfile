FROM apache/airflow:2.8.1

USER root

# Create folders that Airflow can write to
RUN mkdir -p /opt/airflow/data /opt/airflow/logs /opt/airflow/scripts
RUN chown -R airflow: /opt/airflow/data /opt/airflow/logs /opt/airflow/scripts

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
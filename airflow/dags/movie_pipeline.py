from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="movielens_etl",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    download = BashOperator(
        task_id="download_data",
        bash_command="python /opt/airflow/scripts/download_data.py"
    )

    load = BashOperator(
        task_id="load_to_db",
        bash_command="python /opt/airflow/scripts/load_to_db.py"
    )

    clean = BashOperator(
        task_id="clean_data",
        bash_command="python /opt/airflow/scripts/clean.py"
    )

    quality = BashOperator(
        task_id="quality_check",
        bash_command="python /opt/airflow/scripts/quality_check.py"
    )

    warehouse = BashOperator(
        task_id="build_warehouse",
        bash_command="python /opt/airflow/scripts/warehouse.py"
    )

    analytics = BashOperator(
        task_id="run_analytics",
        bash_command="python /opt/airflow/scripts/analytics.py"
    )

    download >> load >> clean >> quality >> warehouse >> analytics

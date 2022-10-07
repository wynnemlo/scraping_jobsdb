import os
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="parse_html_in_spark",
    schedule_interval="@weekly",
    start_date=datetime(2022, 8, 1),
    catchup=False,
    default_args={"retries": 6, "retry_delay": timedelta(minutes=5)},
    tags=["parse"],
) as dag:

    start = EmptyOperator(task_id="start")

    run_parse_html_in_spark = SparkSubmitOperator(
        task_id="run_parse_html_in_spark",
        application="/spark_code/ParseHtml.py",
        conn_id="spark_local",
        jars="/spark_code/postgresql-42.4.1.jar",
        application_args=[
            Variable.get("POSTGRES_USERNAME"),
            Variable.get("POSTGRES_PASSWORD"),
        ],
    )

    start >> run_parse_html_in_spark

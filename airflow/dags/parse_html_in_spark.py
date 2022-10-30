import os
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="parse_html_in_spark",
    schedule_interval="@weekly",
    start_date=datetime(2022, 8, 1),
    catchup=False,
    default_args={"retries": 6, "retry_delay": timedelta(minutes=5)},
    template_searchpath="./dags/sql",
    tags=["parse"],
) as dag:

    start = EmptyOperator(task_id="start")

    create_parsed_job_table = PostgresOperator(
        task_id="create_parsed_job_table",
        postgres_conn_id="app_db",
        sql="parse_html_in_spark_create_parsed_job_table.sql",
    )

    run_parse_html_in_spark = SparkSubmitOperator(
        task_id="run_parse_html_in_spark",
        application="/spark_code/ParseHtml.py",
        conn_id="spark_local",
        jars="/spark_code/postgresql-42.4.1.jar",
        py_files="/spark_code/dist/ParseHtml-0.1-py3.10.egg",
        application_args=[
            Variable.get("POSTGRES_USERNAME"),
            Variable.get("POSTGRES_PASSWORD"),
        ],
    )

    start >> create_parsed_job_table >> run_parse_html_in_spark

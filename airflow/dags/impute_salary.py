import os
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="impute_salary",
    schedule_interval="@weekly",
    start_date=datetime(2022, 8, 1),
    catchup=False,
    default_args={"retries": 6, "retry_delay": timedelta(minutes=5)},
    template_searchpath="./dags/sql",
) as dag:

    create_salary_table = PostgresOperator(
        task_id="create_salary_table",
        postgres_conn_id="app_db",
        sql="impute_salary_create_salary_table.sql",
    )

    update_salary_fields = PostgresOperator(
        task_id="update_salary_fields",
        postgres_conn_id="app_db",
        sql="impute_salary_update_parsed_jobs_table.sql",
    )

    create_salary_table >> update_salary_fields

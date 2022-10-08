import os
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook, PostgresOperator

CSV_PATH = f"{os.environ.get('RAW_DATA_DIR')}/result_big_table/result.csv"


def _export_to_csv(**kwargs):
    """
    Dumps the processed job data to a CSV.
    """
    print("raw data dir", os.environ.get("RAW_DATA_DIR"))
    # open connection to Postgres
    postgres_hook = PostgresHook(postgres_conn_id="app_db")
    conn = postgres_hook.get_conn()
    cur = conn.cursor()
    cur.execute("SET search_path TO staging")

    # copy the CSV data to its corresponding table in Postgres
    with open(CSV_PATH, "w", encoding="UTF8") as file:
        cur.copy_to(file=file, table="parsed_jobs", null="")
    conn.commit()


with DAG(
    dag_id="export_to_csv",
    schedule_interval="@weekly",
    start_date=datetime(2022, 8, 1),
    catchup=False,
    default_args={"retries": 6, "retry_delay": timedelta(minutes=5)},
    template_searchpath="./dags/sql",
) as dag:

    export_to_csv = PythonOperator(
        task_id="export_to_csv",
        python_callable=_export_to_csv,
    )

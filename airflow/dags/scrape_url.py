import os
import random
import time
from datetime import datetime

import cloudscraper
from bs4 import BeautifulSoup

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook, PostgresOperator

RAW_DATA_DIR = "/usr/local/airflow/data/staging"
DATA_LAKE_DIR = "./data_lake"
JOBSDB_URL_PREFIX = "https://hk.jobsdb.com"
SALARY_LIST = [
    (11000, 15000),
    (15000, 20000),
    (20000, 30000),
    (30000, 40000),
    (40000, 50000),
    (50000, 60000),
    (60000, 80000),
    (80000, 120000),
]
KEYWORD_LIST = [
    "data_analyst",
    "data_scientist",
    "analytics_manager",
    "data_engineer",
    "data_governance",
    "data_steward",
    "data_management",
    "machine_learning",
]


def get_request_with_keyword(keyword, salary_min, salary_max, page_num) -> str:
    scraper = cloudscraper.create_scraper()
    url = f"{JOBSDB_URL_PREFIX}/hk/search-jobs/{keyword.replace('_', '-')}/{page_num}?SalaryF={salary_min}&SalaryT={salary_max}&SalaryType=1"
    print("url: ", url)
    return scraper.get(url)


def get_request_with_url(url: str) -> str:
    scraper = cloudscraper.create_scraper()
    url = f"{JOBSDB_URL_PREFIX}/{url}"
    print("url: ", url)
    return scraper.get(url)


def get_csv_filename(keyword: str, salary_min: str, salary_max: str, date: str) -> str:
    filename = f"{RAW_DATA_DIR}/{keyword}_{salary_min}_{salary_max}_{date}.csv"
    dirname = os.path.dirname(filename)
    if not os.path.exists(dirname):
        os.makedirs(dirname)
    return filename


def get_data_lake_filename(job_id: str, url: str, date: str) -> str:
    filename = f"{DATA_LAKE_DIR}/{date[0:4]}/{date[4:6]}/{date[6:8]}/{job_id}.html"
    dirname = os.path.dirname(filename)
    if not os.path.exists(dirname):
        os.makedirs(dirname)
    return filename


def sleep_for_random_seconds():
    time.sleep(random.randint(20, 60))


def scrape_all_pages(
    keyword: str, salary_min: str, salary_max: str, date: str, num_of_pages: int
) -> str:
    import re

    # create file to store data
    csv_file = get_csv_filename(keyword, salary_min, salary_max, date)

    # loop through all pages and add each page's data to csv file
    with open(csv_file, "w") as fp:
        for n in range(1, num_of_pages):
            print("Sleep...")
            sleep_for_random_seconds()

            req = get_request_with_keyword(keyword, salary_min, salary_max, n)
            bs = BeautifulSoup(req.text, "html.parser")
            job_list_container = bs.find("div", {"id": "jobList"})

            print("Grabbing all job links in current page...")
            jobs = job_list_container.find_all(
                "a", {"href": re.compile("\/hk\/en\/job.*")}
            )

            for job in jobs:
                job_url = job["href"]
                job_id = re.search("\-(\d*$)", job_url).group(1)

                fp.write(
                    "%s\n"
                    % f"{keyword},{job_id},{salary_min},{salary_max},{date},{job_url}"
                )

    # return os.path.basename(csv_file)
    return csv_file


def _scrape(keyword: str, salary_min: str, salary_max: str, **kwargs):
    import json
    import re

    # get first page and get number of pages
    # access left container
    req = get_request_with_keyword(keyword, salary_min, salary_max, 1)
    bs = BeautifulSoup(req.text, "html.parser")
    job_list_container = bs.find("div", {"id": "jobList"})

    # get number of pages
    num_of_pages = json.loads(job_list_container["data-sol-meta"])["pageSize"]
    print("num of pages: ", num_of_pages)

    return scrape_all_pages(keyword, salary_min, salary_max, kwargs["ds"], num_of_pages)


def _copy_data_from_csv(keyword: str, salary_min: str, salary_max: str, **kwargs):
    postgres_hook = PostgresHook(postgres_conn_id="app_db")
    conn = postgres_hook.get_conn()
    cur = conn.cursor()
    cur.execute("SET search_path TO raw, public")
    csv_file = kwargs["ti"].xcom_pull(
        task_ids=f"scrape_{keyword}_{salary_min}_{salary_max}"
    )
    print("filename: ", csv_file)
    with open(csv_file, "r") as file:
        cur.copy_from(
            file,
            f"{keyword}_{salary_min}_{salary_max}_raw",
            columns=[
                "keyword",
                "job_id",
                "salary_min",
                "salary_max",
                "scrape_date",
                "url",
            ],
            sep=",",
        )
    conn.commit()


def _do_scrape(**kwargs):
    postgres_hook = PostgresHook(postgres_conn_id="app_db")
    conn = postgres_hook.get_conn()
    cur = conn.cursor()
    cur.execute("SELECT job_id, url FROM raw.scraped_job WHERE file_path IS NULL;")
    jobs_to_scrape = cur.fetchall()
    if not jobs_to_scrape:
        raise Exception("No data")

    for job in jobs_to_scrape:
        job_id = job[0]
        url = job[1]
        html = get_request_with_url(url).text

        # validate returned html is valid
        bs = BeautifulSoup(html, "html.parser")
        job_title = bs.find("div", {"data-automation": "detailsTitle"}).h1.get_text()
        if job_title == None:
            raise Exception("Scraper returning unexpected HTML")

        filename = get_data_lake_filename(job_id, url, kwargs["ds_nodash"])
        print(filename)

        with open(filename, "w") as file:
            file.write(html)

        cur.execute(
            f"UPDATE raw.scraped_job SET scraped_date = current_date, file_path = '{filename}' WHERE job_id = '{job_id}';"
        )
        conn.commit()

        sleep_for_random_seconds


with DAG(
    dag_id="scrape_url",
    schedule_interval="@daily",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    template_searchpath="./dags/sql",
    tags=["scrape"],
) as dag:

    start = DummyOperator(task_id="start")

    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="app_db",
        sql="scrape_url_create_raw_table.sql",
    )

    dedupe_jobs = PostgresOperator(
        task_id="dedupe_jobs",
        postgres_conn_id="app_db",
        sql="scrape_url_dedupe_jobs.sql",
    )

    for keyword in KEYWORD_LIST:
        for salary_min, salary_max in SALARY_LIST:
            scrape_url = PythonOperator(
                task_id=f"scrape_{keyword}_{salary_min}_{salary_max}",
                python_callable=_scrape,
                op_kwargs={
                    "keyword": keyword,
                    "salary_min": salary_min,
                    "salary_max": salary_max,
                },
            )

            create_temp_table = PostgresOperator(
                task_id=f"create_temp_table_{keyword}_{salary_min}_{salary_max}",
                postgres_conn_id="app_db",
                sql="scrape_url_create_temp_table.sql",
                params={
                    "keyword": keyword,
                    "salary_min": salary_min,
                    "salary_max": salary_max,
                },
            )

            copy_data = PythonOperator(
                task_id=f"copy_{keyword}_{salary_min}_{salary_max}",
                python_callable=_copy_data_from_csv,
                op_kwargs={
                    "keyword": keyword,
                    "salary_min": salary_min,
                    "salary_max": salary_max,
                },
            )

            insert_data = PostgresOperator(
                task_id=f"insert_{keyword}_{salary_min}_{salary_max}",
                postgres_conn_id="app_db",
                sql="scrape_url_insert_data.sql",
                params={
                    "keyword": keyword,
                    "salary_min": salary_min,
                    "salary_max": salary_max,
                },
            )

            drop_temp_table = PostgresOperator(
                task_id=f"drop_{keyword}_{salary_min}_{salary_max}",
                postgres_conn_id="app_db",
                sql="scrape_url_drop_temp_table.sql",
                params={
                    "keyword": keyword,
                    "salary_min": salary_min,
                    "salary_max": salary_max,
                },
            )

            (
                create_table
                >> scrape_url
                >> create_temp_table
                >> copy_data
                >> insert_data
                >> drop_temp_table
                >> dedupe_jobs
            )

    # after dedupe jobs, scrape each url and store it into data lake
    # create table called "scraped_jobs" that serves as directory to data lake
    # copy over all records that don't yet exist in the scraped_jobs table
    create_scraped_jobs_table = PostgresOperator(
        task_id="create_scraped_jobs_table",
        postgres_conn_id="app_db",
        sql="scrape_url_create_scraped_job_table.sql",
    )

    do_scrape = PythonOperator(
        task_id="do_scrape",
        python_callable=_do_scrape,
    )

    start >> create_table
    dedupe_jobs >> create_scraped_jobs_table >> do_scrape

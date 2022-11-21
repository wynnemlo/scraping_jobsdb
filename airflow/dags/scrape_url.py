import os
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.common.sql.operators.sql import SQLColumnCheckOperator
from airflow.providers.postgres.operators.postgres import PostgresHook, PostgresOperator
from airflow.utils.trigger_rule import TriggerRule

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
    "machine_learning",
    "data_governance",
    "dwh",
    "etl",
    "snowflake",
    "databricks",
    "oracle",
]


def _make_get_request_with_keyword(
    keyword: str, salary_min: str, salary_max: str, page_num: str
) -> str:
    """ "Given a keyword, min salary and max salary, makes a GET request to JobsDB and does a search for jobs posted in the last 7 days with the given parameters. It will bypass Cloudflare blocker and return the HTML body of the GET request's response

    Args:
        keyword (str): keyword to use for job search, connected by underscore, e.g. 'data_analyst'
        salary_min (str): minimum salary for the job search in HKD, e.g. '20000'
        salary_max (str): maximum salary for the job search in HKD, e.g. '20000'
        page_num (str): _description_

    Returns:
        str: HTML body returned from the GET request
    """
    import cloudscraper

    scraper = cloudscraper.create_scraper()
    url = f"{JOBSDB_URL_PREFIX}/hk/search-jobs/{keyword.replace('_', '-')}/{page_num}?SalaryF={salary_min}&SalaryT={salary_max}&SalaryType=1&createdAt=30d"
    print("url: ", url)

    response = scraper.get(url)
    return response.text


def _make_get_request_with_url(url: str) -> str:
    """Given a url path for JobsDB, makes a GET request to the url. It will bypass Cloudflare blocker and return the HTML body of the GET request's response.

    Args:
        url (str): The url path. Formatted as: 'hk/en/job/job-name-jobid'

    Returns:
        str: HTML body returned from the GET request
    """
    import cloudscraper

    scraper = cloudscraper.create_scraper()
    url = f"{JOBSDB_URL_PREFIX}{url}"
    print("url: ", url)

    response = scraper.get(url)
    return response.text


def _prepare_file_dir_for_csv(
    keyword: str, salary_min: str, salary_max: str, search_date: str
) -> str:
    """Returns the CSV file path for which the job's metadata should be saved. If the intermediate file directories for the file path do not exist, this function would also create the needed intermediate directories.

    Args:
        keyword (str): Search keyword used
        salary_min (str): Min salary used in the search
        salary_max (str): Max salary used in the search
        search_date (str): Date on which the search was executed in the format of YYYY-MM-DD

    Returns:
        csv_filepath (str): A file path for a CSV file
    """
    csv_filepath = f"{os.environ.get('RAW_DATA_DIR')}/{keyword}_{salary_min}_{salary_max}_{search_date}.csv"
    dirname = os.path.dirname(csv_filepath)
    if not os.path.exists(dirname):
        os.makedirs(dirname)
    return csv_filepath


def _prepare_data_lake_dir_for_html(job_id: str, scrape_date: str) -> str:
    """Returns the file path for which the job should be saved in the data lake. If the intermediate file directories for the file path do not exist, this function would also create the needed intermediate directories.

    Args:
        job_id (str): job ID of the job, assigned from JobsDB
        scrape_date (str): Date on which the scrape was executed in the format of YYYY-MM-DD

    Returns:
        html_filepath (str): A file path for a HTML file
    """

    html_filepath = f"{os.environ.get('DATA_LAKE_DIR')}/{scrape_date[0:4]}/{scrape_date[5:7]}/{scrape_date[8:10]}/{job_id}.html"
    dirname = os.path.dirname(html_filepath)
    if not os.path.exists(dirname):
        os.makedirs(dirname)
    return html_filepath


def _sleep_for_random_seconds():
    """Sleeps for a random number of seconds. Currently set to sleep between 1-6 seconds."""

    import random
    import time

    seconds = random.randint(1, 6)
    print(f"Sleeping for {seconds} seconds.")
    time.sleep(seconds)


def _scrape(keyword: str, salary_min: str, salary_max: str, **kwargs) -> str:
    """Runs a search on JobsDB with the given parameters. It will save each job's URL path and its job_id to a CSV file.

    Args:
        keyword (str): Search keyword used
        salary_min (str): Min salary used in the search
        salary_max (str): Max salary used in the search

    Returns:
        csv_filepath (str): The filepath of the CSV file
    """
    import re

    from bs4 import BeautifulSoup

    # get num of pages
    num_of_pages = kwargs["ti"].xcom_pull(
        task_ids=f"scrape_or_skip_{keyword}_{salary_min}_{salary_max}",
        key="num_of_pages",
    )

    csv_filepath = _prepare_file_dir_for_csv(
        keyword=keyword,
        salary_min=salary_min,
        salary_max=salary_max,
        search_date=kwargs["ds"],
    )

    # loop through all pages and add each page's data to csv file
    with open(csv_filepath, "w") as fp:
        for n in range(1, num_of_pages):
            _sleep_for_random_seconds()

            # make GET request
            text = _make_get_request_with_keyword(keyword, salary_min, salary_max, n)
            bs = BeautifulSoup(text, "html.parser")
            job_list_container = bs.find("div", {"id": "jobList"})

            # locate URLs of the jobs in the page
            jobs = job_list_container.find_all(
                "a", {"href": re.compile("\/hk\/en\/job.*")}
            )

            # save job_id and URLs to csv file
            for job in jobs:
                job_url = job["href"]
                job_id = re.search("\-(\d*$)", job_url).group(1)

                fp.write(
                    "%s\n"
                    % f"{keyword},{job_id},{salary_min},{salary_max},{kwargs['ds']},{job_url}"
                )

    return csv_filepath


def _scrape_or_skip(keyword: str, salary_min: str, salary_max: str, **kwargs):
    """Runs a search on JobsDB with the given parameters; If the search returns 0 results, it will inform the DAG to skip the scraping of this combination of parameters, otherwise it should run a scrape on them.

    Args:
        keyword (str): Search keyword used
        salary_min (str): Min salary used in the search
        salary_max (str): Max salary used in the search

    Returns:
        task_id (str): The filepath of the CSV file
    """

    import json
    import math
    import re

    from bs4 import BeautifulSoup

    # parse HTML with bs
    text = _make_get_request_with_keyword(
        keyword=keyword, salary_min=salary_min, salary_max=salary_max, page_num=1
    )
    bs = BeautifulSoup(text, "html.parser")

    # check if there are 0 results, if so, skip this scrape
    zero_results_page = bs.find("div", {"data-automation": "zeroResultsPage"})
    if zero_results_page != None:
        return "dedupe_jobs"
    else:
        # get number of pages and push to xcom
        job_list_container = bs.find("div", {"id": "jobList"})
        num_of_pages = math.ceil(
            int(json.loads(job_list_container["data-sol-meta"])["totalJobCount"]) / 30
        )
        print("num of pages: ", num_of_pages)
        kwargs["ti"].xcom_push(key="num_of_pages", value=num_of_pages)

        # return task_id to conduct scrape
        return f"scrape_{keyword}_{salary_min}_{salary_max}"


def _copy_data_from_csv(keyword: str, salary_min: str, salary_max: str, **kwargs):
    """Copies the existing CSVs data to the Postgres database.

    Args:
        keyword (str): Search keyword used
        salary_min (str): Min salary used in the search
        salary_max (str): Max salary used in the search
    """

    # open connection to Postgres
    postgres_hook = PostgresHook(postgres_conn_id="app_db")
    conn = postgres_hook.get_conn()
    cur = conn.cursor()
    cur.execute("SET search_path TO raw, public")

    # get CSV filepath from previous task
    csv_filepath = kwargs["ti"].xcom_pull(
        task_ids=f"scrape_{keyword}_{salary_min}_{salary_max}"
    )

    # copy the CSV data to its corresponding table in Postgres
    with open(csv_filepath, "r") as file:
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
    from bs4 import BeautifulSoup

    # open connection to Postgres
    postgres_hook = PostgresHook(postgres_conn_id="app_db")
    conn = postgres_hook.get_conn()
    cur = conn.cursor()

    # get from Postgres all the jobs that are not yet scraped
    cur.execute("SELECT job_id, url FROM raw.scraped_job WHERE file_path IS NULL;")
    jobs_to_scrape = cur.fetchall()
    if not jobs_to_scrape:
        raise Exception("No jobs need to be scraped. Are you sure nothing went wrong?")
    print(f"Scraping {len(jobs_to_scrape)} jobs...")

    # loop through the jobs and make GET requests to them one by one
    for job in jobs_to_scrape:
        job_id = job[0]
        url = job[1]
        html = _make_get_request_with_url(url)

        # validate returned html is valid
        bs = BeautifulSoup(html, "html.parser")
        job_title = bs.find("div", {"data-automation": "detailsTitle"}).h1.get_text()
        if job_title == None:
            raise Exception("Scraper returning unexpected HTML")

        # prepare the path in data lake to save the HTML to
        html_file = _prepare_data_lake_dir_for_html(job_id, kwargs["ds"])
        print(html_file)

        # save HTML
        with open(html_file, "w") as file:
            file.write(html)

        # update metadata table in Postgres
        cur.execute(
            f"UPDATE raw.scraped_job SET scraped_date = current_date, file_path = '{html_file}' WHERE job_id = '{job_id}';"
        )
        conn.commit()

        _sleep_for_random_seconds()


with DAG(
    dag_id="scrape_url",
    schedule_interval="@daily",
    start_date=datetime(2022, 8, 1),
    catchup=False,
    default_args={"retries": 6, "retry_delay": timedelta(minutes=5)},
    template_searchpath="./dags/sql",
    tags=["scrape"],
) as dag:

    start = EmptyOperator(task_id="start")

    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="app_db",
        sql="scrape_url_create_raw_table.sql",
    )

    dedupe_jobs = PostgresOperator(
        task_id="dedupe_jobs",
        postgres_conn_id="app_db",
        sql="scrape_url_dedupe_jobs.sql",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # for each keyword and each salary combination, create corresponding tasks
    for keyword in KEYWORD_LIST:
        for salary_min, salary_max in SALARY_LIST:
            scrape_or_skip = BranchPythonOperator(
                task_id=f"scrape_or_skip_{keyword}_{salary_min}_{salary_max}",
                python_callable=_scrape_or_skip,
                op_kwargs={
                    "keyword": keyword,
                    "salary_min": salary_min,
                    "salary_max": salary_max,
                },
            )

            scrape = PythonOperator(
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
                >> scrape_or_skip
                >> scrape
                >> create_temp_table
                >> copy_data
                >> insert_data
                >> drop_temp_table
                >> dedupe_jobs
            )

    create_scraped_jobs_table = PostgresOperator(
        task_id="create_scraped_jobs_table",
        postgres_conn_id="app_db",
        sql="scrape_url_create_scraped_job_table.sql",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    do_scrape = PythonOperator(
        task_id="do_scrape",
        python_callable=_do_scrape,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    sanity_check = SQLColumnCheckOperator(
        task_id="sanity_check",
        conn_id="app_db",
        table="raw.scraped_job",
        column_mapping={
            "job_id": {"unique_check": {"equal_to": 0}},
            "scraped_date": {
                "min": {"greater_than": date(2022, 7, 1)},
                "max": {"less_than": date(2022, 12, 1)},
            },
            "file_path": {"null_check": {"equal_to": 0}},
        },
    )

    start >> create_table
    dedupe_jobs >> create_scraped_jobs_table >> do_scrape >> sanity_check

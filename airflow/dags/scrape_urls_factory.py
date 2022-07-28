from datetime import datetime

from airflow.operators.python_operator import PythonOperator

from airflow import DAG


def create_dag(
    dag_id, schedule, dag_number, default_args, keyword, salaryMin, salaryMax
):
    def _scrape_job_urls(keyword: str, salaryMin: str, salaryMax: str, **kwargs):
        import json
        import re

        import cloudscraper
        import requests
        from bs4 import BeautifulSoup

        # make request to JobsDB with specified params
        scraper = cloudscraper.create_scraper()
        url = f"https://hk.jobsdb.com/hk/search-jobs/{keyword}/1?SalaryF={salaryMin}&SalaryT={salaryMax}&SalaryType=1"
        print("url: ", url)
        try:
            req = scraper.get(url)
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)

        # access left container
        bs = BeautifulSoup(req.text, "html.parser")
        jobListContainer = bs.find("div", {"id": "jobList"})

        # get number of pages
        numOfPages = json.loads(jobListContainer["data-sol-meta"])["pageSize"]
        print("numOfPages: ", numOfPages)

        # loop through all pages and get all job links
        print("Grabbing all job links in current page...")
        jobList = jobListContainer.find_all(
            "a", {"href": re.compile("\/hk\/en\/job.*")}
        )
        for job in jobList:
            print(job["href"])

    dag = DAG(
        dag_id, schedule_interval=schedule, default_args=default_args, tags=["scrape"]
    )

    with dag:
        t1 = PythonOperator(
            task_id="scrape_job_urls",
            python_callable=_scrape_job_urls,
            op_kwargs={
                "keyword": keyword,
                "salaryMin": salaryMin,
                "salaryMax": salaryMax,
            },
        )

    return dag


# build a dag for each number in range(10)
salaryMin = [11000, 15000, 20000, 30000, 40000, 50000, 60000, 80000]
salaryMax = [15000, 20000, 30000, 40000, 50000, 60000, 80000, 120000]
keywords = [
    "data-analyst",
    "data-scientist",
    "analytics-manager",
    "data-engineer",
    "data-governance",
    "data-steward",
    "data-management",
    "machine-learning",
]

for keyword in keywords:
    for n in range(len(salaryMin)):
        dag_id = f"scrape_{keyword}_{salaryMin[n]}_{salaryMax[n]}"

        default_args = {
            "owner": "airflow",
            "start_date": datetime(2022, 7, 26),
        }

        schedule = "@daily"
        dag_number = n

        globals()[dag_id] = create_dag(
            dag_id,
            schedule,
            dag_number,
            default_args,
            keyword,
            salaryMin[n],
            salaryMax[n],
        )

Pay transparency is a huge issue in Hong Kong. According to an article by [Wired.com](https://www.wired.com/story/salary-transparency-gender-pay-gap/), salary secrecy often leads to unequal compensation for minorities, allowing companies to underpay employees due to various biases. EU is already moving to [make pay transparency a mandatory measure](https://www.euractiv.com/section/economy-jobs/news/eu-negotiators-discuss-scope-of-pay-transparency-directive/) for companies, while Hong Kong is still lagging behind in terms of its social policy and work culture.

I was curious as to what the various pay scales are for data-related jobs, so I set out to scrape one of the most popular job posting website in Hong Kong, JobsDB, and analyze its data.

# Scraping for hidden information - salary!

JobsDB does not show the salary of each job posting publicly. However, they do have an advanced search function that allows us to filter results based on salary.

![1](https://user-images.githubusercontent.com/7219284/198118865-1eb5007b-41c1-4ca4-a6d0-7ec147af7813.png)


Knowing this, our process hereafter is very straightforward. We will create a list of keywords as our search terms, and for each search term, we will want to search it multiple times with the salary set to provided ranges: 

```python

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
```

If the job shows up under the search results for the salary range, that means that that job is within that salary range - for example, if the same job shows up under a search of salary range of 11k - 15k and also 15k-20k, but it doesn’t show up in the search of the 20k-30k, that means the job has an effective salary range of 11k-20k. This way, we are able to get an estimate of the salary offered for every job.

# Architecture overview


![2](https://user-images.githubusercontent.com/7219284/198118913-abf92b9c-4ba6-411c-8f56-56cbe2a75231.png)

- Airflow
    - We will use Airflow as our workflow orchestrator and also run our scraper directly on Airflow workers. In Production settings, it is recommended to use the KubernetesExecutor. Since we’re running this all on a local environment (my computer!), we’re only using SequentialExecutor.
    - We have used Astronomer CLI in this project to save on the setup on Airflow
    - Idempotency! Every DAG and task was written with idempotency in mind, assuming that there will be errors.
    - Not letting errors go silent! When we encounter an error, it is super important for it to throw an exception instead of failing silently. We also make use of sanity tests with SQL to create extra layers of error-checking.
- Data lake
    - We store all the raw data in a data lake first. That means all we do is scrape JobsDB, and save the HTML file in our disk or cloud storage.
    - However, as we can see above, it would also be important to save the metadata of each scrape we do so we are able to match each record to a salary range. All of that metadata is saved in a Postgres database.
- Data transformation
    - After we’ve collected a bunch of HTML files in our data lake, we use Spark to parse and extract the important fields from the HTML files, e.g. the job title, the job type, the career level of the job
    - We save all of the extracted information into a neat, deduplicated table in Postgres database
    - Data is now ready for analysis and machine learning!
- Terraform
    - Terraform here was used as IaC to instantiate the Google Compute Engine and CloudSQL instance. The GCE instances will run Airflow through a Docker container.
    - In reality though, I realized that the cost of running all of this on the cloud will be too high and have mostly been running it on my local machine.

# Airflow overview

We only have 4 DAGs in our project. 

## Data collection: scrape_url

![This is a huge DAG and I’m not very proud of it.](https://user-images.githubusercontent.com/7219284/198118989-528e5b90-2012-439b-b59e-d4f28dcb7b82.png)

The tasks here are dynamically generated - and because they had dependencies on each other, it ended up being a huge monolith DAG! Airflow 2.4 have just been released while I was working on this project, and its new feature Datasets seem to be able to address this problem, which would definitely be an improvement.

These tasks do the following:

- scrape_or_skip_KEYWORD_SALARY: run a search on JobsDB with the keyword combination. Look at the search results - if there are 0 results, skip. Otherwise, mark the number of pages to scrape.
- scrape_KEYWORD_SALARY: iterate through the pages in the search results and record all the individual URLs that need to be scraped. (They’re all saved in a CSV file)
- create_temp_table, copy, insert, drop: insert the records from CSV to Postgres
- dedupe_jobs: because we did multiple searches on the same keyword (for estimating the salary), we need to dedupe the jobs we scrape, so we don’t scrape the same URL twice.
- create_scraped_jobs_table: we save all the URLs to be scraped in a Postgres table.
- do_scrape: this is where we actually hit the marked URLs one by one, and save the HTML file in our data lake, partitioned by year, month, date.
- sanity_check: do a quick sanity check by running some SQL validation tests

After this DAG is run, we have already concluded our data collection phase!

## Data Transformation: parse_html_in_spark

This is a simple DAG. All it does is instruct the Spark machine to kick off a Spark job. You can see our Spark app in the `/spark`  directory.

- It connects to our Postgres database and reads in the table we created above as a dataframe. Each record in the dataframe represents a job, which contains a column that holds the filepath to the HTML file in the data lake.

```python
df = (
        spark.read.format("jdbc")
        .option("url", "jdbc:postgresql://pgdatabase:5432/app_db")
        .option("query", sql_query)
        .option("user", postgres_username)
        .option("password", postgres_password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )
```

- We run it through a UDF that will read the file from the data lake and use BeautifulSoup to parse the HTML

```python
parse_job_udf = udf(parse_job_html, returnType=ArrayType(StringType()))
```

- The resulting dataframe is then saved to a new table in Postgres.

## Data Transformation: impute_salary

As mentioned above, the same job will show up several times in our search results, and we have to use that information to impute the salary range of our job. This DAG simply runs a SQL transformation in Pogstres and creates a table that imputes the salary for each job_id and then attach the salary information to the dataframe created in parse_html_in_spark.

```python
DROP TABLE IF EXISTS "staging".imputed_salary;

CREATE TABLE "staging".imputed_salary AS
SELECT 
	job_id, 
	min(salary_min) as min_salary,
	max(salary_max) as max_salary
FROM raw.raw_scraped_url
GROUP BY job_id;
```

## Export data to CSV: export_to_csv

Export the big table in Postgres to a CSV. In this case, I’ve exported it for the purposes of doing data exploration in Jupyter notebook.

# Data Analysis

Data analysis is not the focus of this project, but some interesting insights were found. We used Kmeans to cluster the jobs into groups, TF-IDF for extracting features from text, and used GradientBoost to try to build a prediction model. Please check this [Kaggle notebook](https://www.kaggle.com/code/wynnelo/hong-kong-data-jobs-2022-analysis) for details.

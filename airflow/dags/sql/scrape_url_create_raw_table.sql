CREATE TABLE IF NOT EXISTS raw.raw_scraped_url
(
    keyword character varying(255) NOT NULL,
    job_id character varying(255) NOT NULL,
    salary_min integer NOT NULL,
    salary_max integer NOT NULL,
    scrape_date date NOT NULL,
    url character varying(500) NOT NULL,
    id SERIAL,
    CONSTRAINT raw_scraped_url_pkey PRIMARY KEY (id),
    CONSTRAINT raw_scraped_url_keyword_job_id_salary_min_salary_max_scrape_key UNIQUE (keyword, job_id, salary_min, salary_max, scrape_date)
)
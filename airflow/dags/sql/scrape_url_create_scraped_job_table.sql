CREATE TABLE IF NOT EXISTS raw.scraped_job
(
    job_id character varying(255) NOT NULL,
    url character varying(500) NOT NULL,
    scraped_date date,
    file_path character varying(500),
    CONSTRAINT scraped_jobs_pkey PRIMARY KEY (job_id)
);

INSERT INTO raw.scraped_job
SELECT raw.unique_job.job_id, raw.unique_job.url
FROM raw.unique_job
LEFT OUTER JOIN raw.scraped_job
    ON (raw.unique_job.job_id = raw.scraped_job.job_id)
    WHERE raw.scraped_job.job_id IS NULL
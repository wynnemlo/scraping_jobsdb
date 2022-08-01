DROP TABLE IF EXISTS raw.unique_job;

CREATE TABLE raw.unique_job AS
SELECT distinct on (job_id)
	keyword,
	job_id,
	url
FROM raw.raw_scraped_url
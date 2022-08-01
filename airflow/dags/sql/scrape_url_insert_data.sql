INSERT INTO raw.raw_scraped_url
SELECT keyword, job_id, salary_min, salary_max, scrape_date, url
FROM raw.{{ params.keyword}}_{{ params.salary_min }}_{{ params.salary_max }}_raw
ON CONFLICT ON CONSTRAINT raw_scraped_url_keyword_job_id_salary_min_salary_max_scrape_key DO NOTHING;
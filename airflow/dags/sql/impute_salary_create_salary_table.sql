CREATE TABLE IF NOT EXISTS "staging".imputed_salary AS
SELECT 
	job_id, 
	min(salary_min) as min_salary,
	max(salary_max) as max_salary
FROM raw.raw_scraped_url
GROUP BY job_id;
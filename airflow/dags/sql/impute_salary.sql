UPDATE staging.parsed_jobs
SET min_salary = salary_info.min_salary,
	max_salary = salary_info.max_salary
FROM 
	(
		SELECT job_id, 
			min(salary_min) as min_salary,
			max(salary_max) as max_salary
		FROM raw.raw_scraped_url
		GROUP BY job_id
	) as salary_info
WHERE salary_info.job_id = staging.parsed_jobs.job_id
	AND staging.parsed_jobs.min_salary is null
	AND staging.parsed_jobs.max_salary is null
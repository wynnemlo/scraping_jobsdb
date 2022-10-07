UPDATE staging.parsed_jobs
SET min_salary = salary_info.min_salary,
	max_salary = salary_info.max_salary
FROM staging.imputed_salary as salary_info
WHERE salary_info.job_id = staging.parsed_jobs.job_id
	AND staging.parsed_jobs.min_salary is null
	AND staging.parsed_jobs.max_salary is null
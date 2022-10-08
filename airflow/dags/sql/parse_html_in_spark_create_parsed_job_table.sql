CREATE TABLE IF NOT EXISTS staging.parsed_jobs
(
    job_id text COLLATE pg_catalog."default" NOT NULL,
    job_title text COLLATE pg_catalog."default",
    company_name text COLLATE pg_catalog."default",
    job_description text COLLATE pg_catalog."default",
    location text COLLATE pg_catalog."default",
    official_post_date date,
    min_official_salary integer,
    max_official_salary integer,
    career_level text COLLATE pg_catalog."default",
    qualification text COLLATE pg_catalog."default",
    job_type text COLLATE pg_catalog."default",
    job_functions text COLLATE pg_catalog."default",
    industry text COLLATE pg_catalog."default",
    min_salary integer,
    max_salary integer,
    CONSTRAINT parsed_jobs_pkey PRIMARY KEY (job_id)
)
DROP TABLE IF EXISTS raw.{{ params.keyword}}_{{ params.salary_min }}_{{ params.salary_max }}_raw;

CREATE TABLE raw.{{ params.keyword}}_{{ params.salary_min }}_{{ params.salary_max }}_raw 
(
    keyword character varying(255) NOT NULL,
    job_id character varying(255) NOT NULL,
    salary_min integer NOT NULL,
    salary_max integer NOT NULL,
    scrape_date date NOT NULL,
    url character varying(500) NOT NULL
);
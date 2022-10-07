import re
from datetime import datetime
from unittest import TestCase

from lib.utils import parse_job_html
from pyspark.sql import SparkSession


class UtilsTestCase(TestCase):
    def test_parse_job_html(self):
        result = parse_job_html("data/sample.html", datetime.today())
        job_title = result[0]
        company_name = result[1]
        job_description = result[2]
        location = result[3]
        official_post_date = result[4]
        min_official_salary = result[5]
        max_official_salary = result[6]
        career_level = result[7]
        qualification = result[8]
        job_type = result[9]
        job_functions_str = result[10]
        industry = result[11]

        self.assertEqual(job_title, "IT Technical Support Engineer")
        self.assertEqual(company_name, "Ogawa Health Care International (HK) Ltd")
        self.assertTrue(
            bool(re.search("^[^<>]+$", job_description)),
            "Job description should not contain html tags",
        )
        self.assertEqual(
            location,
            "Kowloon Bay",
        )
        self.assertEqual(
            official_post_date,
            "2022-10-03",
        )
        self.assertEqual(min_official_salary, "")
        self.assertEqual(max_official_salary, "")
        self.assertEqual(career_level, "Middle")
        self.assertEqual(qualification, "Non-Degree Tertiary")
        self.assertEqual(job_type, "Full Time, Permanent")
        self.assertEqual(
            job_functions_str,
            "Information Technology (IT); Hardware; Support; Technical / Functional Consulting",
        )
        self.assertEqual(industry, "Trading and Distribution")

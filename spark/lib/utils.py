import configparser
import os
from datetime import datetime

from bs4 import BeautifulSoup
from pyspark import SparkConf
from pyspark.sql.types import *


def parse_job_html(file_path, scraped_date) -> ArrayType(StringType()):
    """Parses the given HTML file and extracts the individual fields into an array
    Args:
        file_path (str): a file path to the HTML file
        scraped_date (datetime.date): date on which the file was scraped

    Returns:
        ArrayType(StringType()): an array of the job attributes,
        e.g. ['Data Scientist', 'HSBC', ...]
    """
    print(f"Parsing {file_path}")

    # to accomodate for the format in the database, where the file path is in format of
    # '/usr/local/airflow/datalake/2022/10/08/192382190.html'
    if file_path[11:] == "/usr/local/":
        local_file_path = "../" + file_path[11:]
    else:
        local_file_path = file_path

    with open(local_file_path, encoding="utf8") as f:
        text = f.read()
        bs = BeautifulSoup(text, "html.parser")
        job_title = (
            bs.find("div", {"data-automation": "detailsTitle"}).h1.get_text().strip()
        )
        company_name = (
            bs.find("div", {"data-automation": "detailsTitle"}).span.get_text().strip()
        )
        job_description_div = bs.find("div", {"data-automation": "jobDescription"})
        job_description = (
            job_description_div.div.get_text(separator="\n")
            if job_description_div
            else ""
        )
        location = ""
        official_post_date = ""
        min_official_salary = ""
        max_official_salary = ""

        # parse header info
        header_info = bs.find("div", {"data-automation": "jobDetailsHeader"}).find_all(
            "div", {"class": "sx2jih0 zcydq86a"}
        )

        for child in header_info:
            if child.get_text()[-4:] == " ago":
                official_post_date = scraped_date.strftime("%Y-%m-%d")
            elif child.get_text()[-2:] == "22":
                official_post_date_raw = child.get_text().split(" on ")[1]
                official_post_date = datetime.strptime(
                    official_post_date_raw, "%d-%b-%y"
                ).strftime("%Y-%m-%d")
            elif child.get_text()[-7:].strip() == ("/month"):
                if child.get_text()[0:5] == "Above":
                    min_official_salary = int(
                        child.get_text().split("$")[1][0:-7].replace(",", "")
                    )
                else:
                    min_official_salary = int(
                        child.get_text().split(" - ")[0][3:].replace(",", "")
                    )
                    max_official_salary = int(
                        child.get_text()[:-7].split(" - ")[1][3:].replace(",", "")
                    )
            elif child.get_text().strip() != "":
                location = child.get_text()

        # parse additional info
        additional_info = bs.find("div", string="Additional Information").parent

        career_level_div = additional_info.find("div", string="Career Level")
        career_level = (
            career_level_div.next_sibling.get_text() if career_level_div else ""
        )

        qualification_div = additional_info.find("div", string="Qualification")
        qualification = (
            qualification_div.next_sibling.get_text() if qualification_div else ""
        )

        job_type_div = additional_info.find("div", string="Job Type")
        job_type = job_type_div.next_sibling.get_text() if job_type_div else ""

        job_functions_div = additional_info.find("div", string="Job Functions")
        job_functions = (
            job_functions_div.next_sibling.get_text() if job_functions_div else ""
        )
        job_functions_str = job_functions.strip().replace(",", ";")

        additional_company_info = bs.find(
            "div", string="Additional Company Information"
        )
        industry_div = (
            additional_company_info.parent.find("div", string="Industry")
            if additional_company_info
            else None
        )
        industry = industry_div.next_sibling.get_text() if industry_div else ""

        result = [
            job_title,
            company_name,
            job_description,
            location,
            official_post_date,
            min_official_salary,
            max_official_salary,
            career_level,
            qualification,
            job_type,
            job_functions_str,
            industry,
        ]
        print(result)

        return result


def get_spark_app_config() -> SparkConf:
    cwd = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    conf_path = os.path.join(cwd, "spark.conf")

    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read(conf_path)

    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf

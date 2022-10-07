import os

from lib.logger import Log4j
from lib.utils import *
from pyspark.sql import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    logger = Log4j(spark)

    if len(sys.argv) != 3:
        logger.error("Usage: ParseHtml <postgres_username> <postgres_password>")
        sys.exit(-1)

    postgres_username = sys.argv[1]
    postgres_password = sys.argv[2]

    logger.info("Starting Parse HTML")

    sql_query = """
            select job_id, file_path, scraped_date
            from raw.scraped_job 
            where
                job_id not in
                (
                    select job_id from staging.parsed_jobs
                )
            """

    df = (
        spark.read.format("jdbc")
        .option("url", "jdbc:postgresql://pgdatabase:5432/app_db")
        .option("query", sql_query)
        .option("user", postgres_username)
        .option("password", postgres_password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    parse_job_udf = udf(parse_job_html, returnType=ArrayType(StringType()))

    df_2 = (
        df.select(
            "job_id",
            *[parse_job_udf("file_path", "scraped_date")[i] for i in range(0, 12)]
        )
        .toDF(
            "job_id",
            "job_title",
            "company_name",
            "job_description",
            "location",
            "official_post_date",
            "min_official_salary",
            "max_official_salary",
            "career_level",
            "qualification",
            "job_type",
            "job_functions",
            "industry",
        )
        .withColumn("official_post_date", col("official_post_date").cast(DateType()))
        .withColumn(
            "min_official_salary", col("min_official_salary").cast(IntegerType())
        )
        .withColumn(
            "max_official_salary", col("max_official_salary").cast(IntegerType())
        )
    )

    df_2.write.format("jdbc").mode("append").option(
        "url", "jdbc:postgresql://pgdatabase:5432/app_db"
    ).option("dbtable", "staging.parsed_jobs").option("user", postgres_username).option(
        "password", postgres_password
    ).option(
        "driver", "org.postgresql.Driver"
    ).save()

    logger.info("Finished Parse Html")
    spark.stop()

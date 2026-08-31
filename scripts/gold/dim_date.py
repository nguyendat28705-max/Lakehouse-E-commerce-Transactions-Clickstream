from pyspark.sql import DataFrame, SparkSession


START_DATE = "1950-01-01"
END_DATE = "2050-12-31"


def build(spark: SparkSession) -> DataFrame:
    return spark.sql(
        f"""
        SELECT
            CAST(date_format(full_date, 'yyyyMMdd') AS INT) AS date_key,
            full_date,
            year(full_date) AS year,
            quarter(full_date) AS quarter,
            month(full_date) AS month,
            dayofmonth(full_date) AS day_of_month,
            dayofweek(full_date) AS day_of_week
        FROM (
            SELECT explode(
                sequence(
                    to_date('{START_DATE}'),
                    to_date('{END_DATE}'),
                    interval 1 day
                )
            ) AS full_date
        )
        """
    )


from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when

from gold_utils import build_scd2_dimension


def build(
    customers_df: DataFrame,
    process_date: str,
    process_ts: str,
    existing_df: DataFrame | None,
) -> DataFrame:
    process_year = int(process_date[:4])

    source = (
        customers_df.where(col("customer_id").isNotNull())
        .withColumn("country_name", col("country"))
        .withColumn(
            "birth_year",
            when(
                col("age").isNotNull() & (col("age") >= 0) & (col("age") <= 150),
                lit(process_year) - col("age"),
            ).otherwise(lit(None).cast("int")),
        ).select(
            "customer_id",
            "name",
            "email",
            "country_name",
            "birth_year",
            "signup_date",
            "marketing_opt_in",
        )
    )

    return build_scd2_dimension(
        source_df=source,
        natural_key="customer_id",
        surrogate_key="customer_key",
        attribute_cols=[
            "name",
            "email",
            "country_name",
            "birth_year",
            "signup_date",
            "marketing_opt_in",
        ],
        process_ts=process_ts,
        existing_df=existing_df,
    )


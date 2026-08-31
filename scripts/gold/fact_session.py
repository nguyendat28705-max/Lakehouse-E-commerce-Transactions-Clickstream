from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from gold_utils import assign_surrogate_key, attach_dimension_key, format_timestamp

def build(
    sessions_df: DataFrame,
    dim_date: DataFrame,
    dim_country: DataFrame,
    dim_source: DataFrame,
    dim_device: DataFrame,
    current_customers: DataFrame,
) -> DataFrame:
    fact = (
        sessions_df.join(current_customers.select("customer_id", "customer_key"), on="customer_id", how="left")
        .withColumn("country_name", col("country"))
        .transform(
            lambda df: attach_dimension_key(
                df, "session_date", dim_date, "full_date", "date_key", "date_key", "session_date_lookup"
            )
        )
        .transform(
            lambda df: attach_dimension_key(
                df, "device", dim_device, "device_name", "device_key", "device_key", "session_device"
            )
        )
        .transform(
            lambda df: attach_dimension_key(
                df, "source", dim_source, "source_name", "source_key", "source_key", "session_source"
            )
        )
        .transform(
            lambda df: attach_dimension_key(
                df, "country_name", dim_country, "country_name", "country_key", "country_key", "session_country"
            )
        )
        .withColumn("start_time", format_timestamp(col("start_time")))
        .select(
            "session_id",
            "customer_key",
            "start_time",
            "date_key",
            "device_key",
            "source_key",
            "country_key",
        )
    )

    return (
        assign_surrogate_key(fact, "session_key")
        .select(
            "session_key",
            "session_id",
            "customer_key",
            "start_time",
            "date_key",
            "device_key",
            "source_key",
            "country_key",
        )
    )


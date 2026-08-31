from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lead, lit, when
from pyspark.sql.window import Window

from gold_utils import assign_surrogate_key, attach_dimension_key


def build(
    events_with_customer: DataFrame,
    dim_date: DataFrame,
    current_customers: DataFrame,
    current_products: DataFrame,
) -> DataFrame:
    session_window = Window.partitionBy("session_id").orderBy(col("timestamp"), col("event_id"))

    fact = (
        events_with_customer.join(
            current_customers.select("customer_id", "customer_key"),
            on="customer_id",
            how="left",
        )
        .join(current_products.select("product_id", "product_key"), on="product_id", how="left")
        .transform(
            lambda df: attach_dimension_key(
                df, "event_date", dim_date, "full_date", "date_key", "date_key", "web_event_date"
            )
        )
        .withColumn("next_timestamp", lead("timestamp").over(session_window))
        .withColumn(
            "duration",
            when(col("next_timestamp").isNull(), lit(0.0))
            .when(col("next_timestamp").cast("long") < col("timestamp").cast("long"), lit(0.0))
            .otherwise((col("next_timestamp").cast("long") - col("timestamp").cast("long")).cast("double")),
        )
        .select(
            "event_id",
            "session_id",
            "customer_key",
            "product_key",
            "date_key",
            "event_type",
            "duration",
        )
    )

    return (
        assign_surrogate_key(fact, "event_key")
        .select(
            "event_key",
            "event_id",
            "session_id",
            "customer_key",
            "product_key",
            "date_key",
            "event_type",
            "duration",
        )
    )


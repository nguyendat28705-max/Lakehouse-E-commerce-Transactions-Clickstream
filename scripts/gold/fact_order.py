from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, lit, round, sum

from gold_utils import assign_surrogate_key, attach_dimension_key, format_timestamp

def build(
    orders_df: DataFrame,
    order_items_df: DataFrame,
    dim_date: DataFrame,
    dim_country: DataFrame,
    dim_source: DataFrame,
    dim_device: DataFrame,
    dim_payment_method: DataFrame,
    current_customers: DataFrame,
    current_products: DataFrame,
) -> DataFrame:
    order_costs = (
        order_items_df.join(
            current_products.select("product_id", "cost_usd"),
            on="product_id",
            how="left",
        )
        .withColumn("item_cost_usd", coalesce(col("cost_usd"), lit(0.0)) * col("quantity"))
        .groupBy("order_id")
        .agg(sum("item_cost_usd").alias("order_cost_usd"))
    )

    fact = (
        orders_df.join(order_costs, on="order_id", how="left")
        .join(current_customers.select("customer_id", "customer_key"), on="customer_id", how="left")
        .withColumn("country_name", col("country"))
        .transform(
            lambda df: attach_dimension_key(
                df, "order_date", dim_date, "full_date", "date_key", "date_key", "order_date"
            )
        )
        .transform(
            lambda df: attach_dimension_key(
                df, "country_name", dim_country, "country_name", "country_key", "country_key", "order_country"
            )
        )
        .transform(
            lambda df: attach_dimension_key(
                df, "source", dim_source, "source_name", "source_key", "source_key", "order_source"
            )
        )
        .transform(
            lambda df: attach_dimension_key(
                df, "device", dim_device, "device_name", "device_key", "device_key", "order_device"
            )
        )
        .transform(
            lambda df: attach_dimension_key(
                df,
                "payment_method",
                dim_payment_method,
                "payment_method_name",
                "payment_method_key",
                "payment_method_key",
                "order_payment",
            )
        ).select(
            "order_id",
            "customer_key",
            "date_key",
            "country_key",
            "source_key",
            "device_key",
            format_timestamp(col("order_time")).alias("order_time"),
            "payment_method_key",
            "discount_pct",
            col("subtotal_usd").alias("gross_revenue_usd"),
            round(col("computed_total") - coalesce(col("order_cost_usd"), lit(0.0)), 2).alias("net_margin_usd"),
        )
    )

    return (
        assign_surrogate_key(fact, "order_key")
        .select(
            "order_key",
            "order_id",
            "customer_key",
            "date_key",
            "country_key",
            "source_key",
            "device_key",
            "order_time",
            "payment_method_key",
            "discount_pct",
            "gross_revenue_usd",
            "net_margin_usd",
        )
    )


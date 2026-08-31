from pyspark.sql import DataFrame
from pyspark.sql.functions import col, min as min

from gold_utils import assign_surrogate_key, attach_dimension_key


def build(
    events_with_customer: DataFrame,
    orders_df: DataFrame,
    order_items_df: DataFrame,
    dim_date: DataFrame,
    current_customers: DataFrame,
    current_products: DataFrame,
) -> DataFrame:
    page_view_dates = (
        events_with_customer.where(
            (col("event_type") == "page_view") & col("product_id").isNotNull()
        )
        .groupBy("customer_id", "product_id")
        .agg(min("event_date").alias("first_view_date"))
    )

    add_to_cart_dates = (
        events_with_customer.where(
            (col("event_type") == "add_to_cart") & col("product_id").isNotNull()
        )
        .groupBy("customer_id", "product_id")
        .agg(min("event_date").alias("first_add_to_cart_date"))
    )

    purchase_events = events_with_customer.where(col("event_type") == "purchase").select(
        "session_id",
        "customer_id",
        col("timestamp").alias("purchase_ts"),
    )

    session_order_map = (
        purchase_events.alias("purchase")
        .join(
            orders_df.select("order_id", "customer_id", "order_time", "order_date").alias("orders"),
            on=(
                (col("purchase.customer_id") == col("orders.customer_id"))
                & (col("purchase.purchase_ts") == col("orders.order_time"))
            ),
            how="inner",
        )
        .select(
            col("purchase.session_id").alias("session_id"),
            col("purchase.customer_id").alias("customer_id"),
            col("orders.order_id").alias("order_id"),
            col("orders.order_date").alias("order_date"),
        )
        .dropDuplicates(["session_id", "order_id"])
    )

    purchase_dates = (
        session_order_map.join(
            order_items_df.select("order_id", "product_id"),
            on="order_id",
            how="inner",
        )
        .groupBy("customer_id", "product_id")
        .agg(min("order_date").alias("first_purchase_date"))
    )

    funnel_base = (
        page_view_dates.select("customer_id", "product_id")
        .unionByName(add_to_cart_dates.select("customer_id", "product_id"))
        .unionByName(purchase_dates.select("customer_id", "product_id"))
        .dropDuplicates(["customer_id", "product_id"])
    )

    fact = (
        funnel_base.join(page_view_dates, on=["customer_id", "product_id"], how="left")
        .join(add_to_cart_dates, on=["customer_id", "product_id"], how="left")
        .join(purchase_dates, on=["customer_id", "product_id"], how="left")
        .join(current_customers.select("customer_id", "customer_key"), on="customer_id", how="left")
        .join(current_products.select("product_id", "product_key"), on="product_id", how="left")
        .transform(
            lambda df: attach_dimension_key(
                df,
                "first_view_date",
                dim_date,
                "full_date",
                "date_key",
                "first_view_date_key",
                "funnel_view",
            )
        )
        .transform(
            lambda df: attach_dimension_key(
                df,
                "first_add_to_cart_date",
                dim_date,
                "full_date",
                "date_key",
                "first_add_to_cart_date_key",
                "funnel_cart",
            )
        )
        .transform(
            lambda df: attach_dimension_key(
                df,
                "first_purchase_date",
                dim_date,
                "full_date",
                "date_key",
                "first_purchase_date_key",
                "funnel_purchase",
            )
        )
        .where(col("customer_key").isNotNull() & col("product_key").isNotNull())
        .select(
            "customer_key",
            "product_key",
            "first_view_date_key",
            "first_add_to_cart_date_key",
            "first_purchase_date_key",
        )
    )

    return (
        assign_surrogate_key(fact, "funnel_key")
        .select(
            "funnel_key",
            "customer_key",
            "product_key",
            "first_view_date_key",
            "first_add_to_cart_date_key",
            "first_purchase_date_key",
        )
    )


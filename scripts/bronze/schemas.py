from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType


def get_events_schema() -> StructType:
    """Define schema of events dataset."""
    return StructType([
        StructField("event_id", IntegerType(), True),
        StructField("session_id", IntegerType(), True),
        StructField("timestamp", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", DoubleType(), True),
        StructField("qty", DoubleType(), True),
        StructField("cart_size", DoubleType(), True),
        StructField("payment", StringType(), True),
        StructField("discount_pct", DoubleType(), True),
        StructField("amount_usd", DoubleType(), True),
    ])


def get_customers_schema() -> StructType:
    """Define schema of customers dataset."""
    return StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("signup_date", StringType(), True),
        StructField("marketing_opt_in", StringType(), True),
    ])


def get_orders_schema() -> StructType:
    """Define schema of orders dataset."""
    return StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("order_time", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("discount_pct", DoubleType(), True),
        StructField("subtotal_usd", DoubleType(), True),
        StructField("total_usd", DoubleType(), True),
        StructField("country", StringType(), True),
        StructField("device", StringType(), True),
        StructField("source", StringType(), True),
    ])


def get_products_schema() -> StructType:
    """Define schema of products dataset."""
    return StructType([
        StructField("product_id", IntegerType(), True),
        StructField("category", StringType(), True),
        StructField("name", StringType(), True),
        StructField("price_usd", DoubleType(), True),
        StructField("cost_usd", DoubleType(), True),
        StructField("margin_usd", DoubleType(), True),
    ])


def get_reviews_schema() -> StructType:
    """Define schema of reviews dataset."""
    return StructType([
        StructField("review_id", IntegerType(), True),
        StructField("order_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("rating", IntegerType(), True),
        StructField("review_text", StringType(), True),
        StructField("review_time", StringType(), True),
    ])


def get_sessions_schema() -> StructType:
    """Define schema of sessions dataset."""
    return StructType([
        StructField("session_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("start_time", StringType(), True),
        StructField("device", StringType(), True),
        StructField("source", StringType(), True),
        StructField("country", StringType(), True),
    ])


def get_order_items_schema() -> StructType:
    """Define schema of order items dataset."""
    return StructType([
        StructField("order_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("unit_price_usd", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("line_total_usd", DoubleType(), True),
    ])


SCHEMA_FACTORIES = {
    "events": get_events_schema,
    "customers": get_customers_schema,
    "orders": get_orders_schema,
    "products": get_products_schema,
    "reviews": get_reviews_schema,
    "sessions": get_sessions_schema,
    "order_items": get_order_items_schema,
}


def get_schema_for_dataset(dataset_name: str):
    """Return the schema for a given dataset."""
    schema_factory = SCHEMA_FACTORIES.get(dataset_name)
    if schema_factory is None:
        return None
    return schema_factory()

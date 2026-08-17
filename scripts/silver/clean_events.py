from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, lit, to_timestamp, when

from silver_utils import safe_trim


def clean_events_df(df: DataFrame, process_date: str) -> DataFrame:
    process_date_col = lit(process_date).cast("date")

    df = safe_trim(df, ["event_type", "payment"])
    
    df = df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"))
    df = df.withColumn("qty", when(col("qty").isNotNull(), col("qty").cast("int")))
    df = df.withColumn("cart_size", when(col("cart_size").isNotNull(), col("cart_size").cast("int")))
    df = df.withColumn("product_id", when(col("product_id").isNotNull(), col("product_id").cast("int")))

    df = df.withColumn("validation_errors", concat_ws(",",
        when(col("event_id").isNull(), "NULL_EVENT_ID"),
        when((col("event_id").isNotNull()) & (col("event_id") <= 0), "INVALID_EVENT_ID"),
        
        when(col("session_id").isNull(), "NULL_SESSION_ID"),
        when((col("session_id").isNotNull()) & (col("session_id") <= 0), "INVALID_SESSION_ID"),
        
        when(col("timestamp").isNull(), "NULL_EVENT_TIMESTAMP"),
        when(col("timestamp").cast("date") > process_date_col, "FUTURE_EVENT_TIMESTAMP"),
        
        when(col("event_type").isNull(), "NULL_EVENT_TYPE"),
        when(~col("event_type").isin("page_view", "add_to_cart", "checkout", "purchase"), "INVALID_EVENT_TYPE"),
        
        when((col("event_type") == "add_to_cart") & col("qty").isNull(), "ADD_TO_CART_MISSING_QTY"),
        when((col("event_type") == "checkout") & col("cart_size").isNull(), "CHECKOUT_MISSING_CART_SIZE"),
        when((col("event_type") == "purchase") & col("amount_usd").isNull(), "PURCHASE_MISSING_AMOUNT"),
        when((col("event_type") == "purchase") & col("payment").isNull(), "PURCHASE_MISSING_PAYMENT"),
        when((col("event_type").isin("page_view", "add_to_cart")) & col("product_id").isNull(), "MISSING_PRODUCT_ID"),
        
        when((col("event_type") == "add_to_cart") & (col("qty") < 0), "NEGATIVE_QTY"),
        when((col("event_type") == "checkout") & (col("cart_size") < 0), "NEGATIVE_CART_SIZE"),
        when((col("event_type") == "purchase") & ((col("discount_pct") < 0) | (col("discount_pct") > 100)), "INVALID_DISCOUNT_PCT"),
        when((col("event_type") == "purchase") & (col("amount_usd") < 0), "NEGATIVE_AMOUNT"),
    ))

    df = df.withColumn("validation_errors", when(col("validation_errors") == "", None).otherwise(col("validation_errors")))
    
    df = df.withColumn("validation_warnings", lit(None).cast("string"))
    
    return df

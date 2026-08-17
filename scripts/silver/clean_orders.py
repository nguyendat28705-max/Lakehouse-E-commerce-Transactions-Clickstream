from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, lit, to_timestamp, when, abs, round

from silver_utils import safe_trim


def clean_orders_df(df: DataFrame, process_date: str) -> DataFrame:
    process_date_col = lit(process_date).cast("date")

    df = safe_trim(df, ["payment_method", "country", "device", "source"])
    df = df.withColumn("order_time", to_timestamp(col("order_time"), "yyyy-MM-dd'T'HH:mm:ss"))
    
    df = df.withColumn("computed_total", round(col("subtotal_usd") * ((100 - col("discount_pct")) / 100), 2))

    df = df.withColumn("validation_errors", concat_ws(",",
        when(col("order_id").isNull(), "NULL_ORDER_ID"),
        when((col("order_id").isNotNull()) & (col("order_id") <= 0), "INVALID_ORDER_ID"),
        
        when(col("customer_id").isNull(), "NULL_CUSTOMER_ID"),
        when((col("customer_id").isNotNull()) & (col("customer_id") <= 0), "INVALID_CUSTOMER_ID"),
        
        when(col("order_time").isNull(), "NULL_ORDER_TIME"),
        when(col("order_time").cast("date") > process_date_col, "FUTURE_ORDER_TIMESTAMP"),
        
        when(col("discount_pct").isNull(), "NULL_DISCOUNT_PCT"),
        when((col("discount_pct") < 0) | (col("discount_pct") > 100), "INVALID_DISCOUNT_PCT"),
        
        when(col("subtotal_usd").isNull(), "NULL_SUBTOTAL"),
        when(col("subtotal_usd") < 0, "NEGATIVE_SUBTOTAL"),
    ))

    df = df.withColumn("validation_errors", when(col("validation_errors") == "", None).otherwise(col("validation_errors")))
    
    df = df.withColumn("validation_warnings", concat_ws(",",
        when(col("total_usd").isNull(), "NULL_TOTAL"),
        when(col("total_usd") < 0, "NEGATIVE_TOTAL"),
        when(abs(col("total_usd") - col("computed_total")) > 0.01, "TOTAL_MISMATCH"),    
        
        when(col("country").isNull(), "NULL_COUNTRY"),
        when(col("device").isNull(), "NULL_DEVICE"),
        when(col("source").isNull(), "NULL_SOURCE"),
    ))
    
    df = df.withColumn("validation_warnings", when(col("validation_warnings") == "", None).otherwise(col("validation_warnings")))
    
    df = df.withColumn("country", when(col("country").isNull(), lit("Unknown")).otherwise(col("country")))
    df = df.withColumn("source", when(col("source").isNull(), lit("Unknown")).otherwise(col("source")))
    df = df.withColumn("device", when(col("device").isNull(), lit("Unknown")).otherwise(col("device")))
    
    return df

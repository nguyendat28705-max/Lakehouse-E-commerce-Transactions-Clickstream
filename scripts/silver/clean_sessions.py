from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, lit, to_timestamp, when

from silver_utils import safe_trim


def clean_sessions_df(df: DataFrame, process_date: str) -> DataFrame:
    process_date_col = lit(process_date).cast("date")

    df = safe_trim(df, ["device", "source", "country"])
    df = df.withColumn("start_time", to_timestamp(col("start_time"), "yyyy-MM-dd'T'HH:mm:ss"))

    df = df.withColumn("validation_errors", concat_ws(",",
        when(col("session_id").isNull(), "NULL_SESSION_ID"),
        when((col("session_id").isNotNull()) & (col("session_id") <= 0), "INVALID_SESSION_ID"),
        
        when(col("customer_id").isNull(), "NULL_CUSTOMER_ID"),
        when((col("customer_id").isNotNull()) & (col("customer_id") <= 0), "INVALID_CUSTOMER_ID"),
        
        when(col("start_time").isNull(), "NULL_START_TIMESTAMP"),
        when(col("start_time").cast("date") > process_date_col, "FUTURE_START_TIMESTAMP"),
    ))

    df = df.withColumn("validation_errors", when(col("validation_errors") == "", None).otherwise(col("validation_errors")))
    
    df = df.withColumn("validation_warnings", concat_ws(",",
        when(col("country").isNull(), "NULL_COUNTRY"),
        when(col("device").isNull(), "NULL_DEVICE"),
        when(col("source").isNull(), "NULL_SOURCE"),
    ))
        
    df = df.withColumn("validation_warnings", when(col("validation_warnings") == "", None).otherwise(col("validation_warnings")))
    
    df = df.withColumn("country", when(col("country").isNull(), lit("Unknown")).otherwise(col("country")))
    df = df.withColumn("source", when(col("source").isNull(), lit("Unknown")).otherwise(col("source")))
    df = df.withColumn("device", when(col("device").isNull(), lit("Unknown")).otherwise(col("device")))
    
    return df

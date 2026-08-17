from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, lit, to_timestamp, when

from silver_utils import safe_trim


def clean_reviews_df(df: DataFrame, process_date: str) -> DataFrame:
    process_date_col = lit(process_date).cast("date")

    df = safe_trim(df, ["review_text"])
    df = df.withColumn("review_time", to_timestamp(col("review_time"), "yyyy-MM-dd'T'HH:mm:ss"))

    df = df.withColumn("validation_errors", concat_ws(",",
        when(col("review_id").isNull(), "NULL_REVIEW_ID"),
        when((col("review_id").isNotNull()) & (col("review_id") <= 0), "INVALID_REVIEW_ID"),
        
        when(col("order_id").isNull(), "NULL_ORDER_ID"),
        when((col("order_id").isNotNull()) & (col("order_id") <= 0), "INVALID_ORDER_ID"),
        
        when(col("product_id").isNull(), "NULL_PRODUCT_ID"),
        when((col("product_id").isNotNull()) & (col("product_id") <= 0), "INVALID_PRODUCT_ID"),
        
        when(col("rating").isNull(), "NULL_RATING"),
        when((col("rating") < 1) | (col("rating") > 5), "INVALID_RATING"),
        
        when(col("review_time").isNull(), "NULL_REVIEW_TIMESTAMP"),
        when(col("review_time").cast("date") > process_date_col, "FUTURE_REVIEW_TIMESTAMP")
    ))

    df = df.withColumn("validation_errors", when(col("validation_errors") == "", None).otherwise(col("validation_errors")))
    
    df = df.withColumn("validation_warnings", lit(None).cast("string"))
    
    return df

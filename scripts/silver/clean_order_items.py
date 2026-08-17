from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, when, abs, round


def clean_order_items_df(df: DataFrame, process_date: str) -> DataFrame:
    df = df.withColumn("computed_line_total", round(col("unit_price_usd") * col("quantity"), 2))
    
    df = df.withColumn("validation_errors", concat_ws(",",
        when(col("order_id").isNull(), "NULL_ORDER_ID"),
        when((col("order_id").isNotNull()) & (col("order_id") <= 0), "INVALID_ORDER_ID"),
        
        when(col("product_id").isNull(), "NULL_PRODUCT_ID"),
        when((col("product_id").isNotNull()) & (col("product_id") <= 0), "INVALID_PRODUCT_ID"),
        
        when(col("unit_price_usd").isNull(), "NULL_UNIT_PRICE"),
        when(col("unit_price_usd") < 0, "NEGATIVE_UNIT_PRICE"),
        
        when(col("quantity").isNull(), "NULL_QUANTITY"),
        when(col("quantity") != col("quantity").cast("int"), "NON_INTEGER_QUANTITY"),
        when(col("quantity") <= 0, "INVALID_QUANTITY"),
    ))
    
    df = df.withColumn("validation_errors", when(col("validation_errors") == "", None).otherwise(col("validation_errors")))
    
    df = df.withColumn("validation_warnings", concat_ws(",",
        when(col("line_total_usd").isNull(), "NULL_LINE_TOTAL"),
        when(col("line_total_usd") < 0, "NEGATIVE_LINE_TOTAL"),
        when(abs(col("line_total_usd") - col("computed_line_total")) > 0.01, "LINE_TOTAL_MISMATCH")
    ))
    
    df = df.withColumn("validation_warnings", when(col("validation_warnings") == "", None).otherwise(col("validation_warnings")))
    
    return df 
    

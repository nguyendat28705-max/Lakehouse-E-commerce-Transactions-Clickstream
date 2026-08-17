from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, when, abs, round

from silver_utils import safe_trim


def clean_products_df(df: DataFrame, process_date: str) -> DataFrame:
    df = safe_trim(df, ["category", "name"])
    
    df = df.withColumn("computed_margin", round(col("price_usd") - col("cost_usd"), 2))

    df = df.withColumn("validation_errors", concat_ws(",",
        when(col("product_id").isNull(), "NULL_PRODUCT_ID"),
        when((col("product_id").isNotNull()) & (col("product_id") <= 0), "INVALID_PRODUCT_ID"),
        
        when(col("category").isNull(), "NULL_CATEGORY"),
        
        when(col("name").isNull(), "NULL_PRODUCT_NAME"),
        
        when(col("price_usd").isNull(), "NULL_PRICE"),
        when(col("price_usd") < 0, "NEGATIVE_PRICE"),
        
        when(col("cost_usd").isNull(), "NULL_COST"),
        when(col("cost_usd") < 0, "NEGATIVE_COST"),
        when(col("cost_usd") > col("price_usd"), "COST_EXCEEDS_PRICE")
    ))

    df = df.withColumn("validation_errors", when(col("validation_errors") == "", None).otherwise(col("validation_errors")))
    
    df = df.withColumn("validation_warnings", concat_ws(",",
            when(col("margin_usd") <= 0, "INVALID_MARGIN"),
            when(abs(col("margin_usd") - col("computed_margin")) > 0.01, "MARGIN_MISMATCH")
        ))
    
    df = df.withColumn("validation_warnings", when(col("validation_warnings") == "", None).otherwise(col("validation_warnings")))
    
    return df

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from gold_utils import build_static_dimension

def build(orders_df: DataFrame, existing_df: DataFrame | None) -> DataFrame:
    source_values = orders_df.select(col("payment_method").alias("payment_method_name"))
    return build_static_dimension(source_values, "payment_method_name", "payment_method_key", existing_df)
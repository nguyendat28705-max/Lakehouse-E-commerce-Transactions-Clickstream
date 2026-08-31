from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from gold_utils import build_static_dimension


def build(products_df: DataFrame, existing_df: DataFrame | None) -> DataFrame:
    source_values = products_df.select(col("category").alias("category_name"))
    return build_static_dimension(source_values, "category_name", "category_key", existing_df) 
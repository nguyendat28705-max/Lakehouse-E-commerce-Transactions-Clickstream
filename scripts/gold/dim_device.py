from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from gold_utils import build_static_dimension

def build(sessions_df: DataFrame, existing_df: DataFrame | None) -> DataFrame:
    source_values = sessions_df.select(col("device").alias("device_name"))
    return build_static_dimension(source_values, "device_name", "device_key", existing_df)
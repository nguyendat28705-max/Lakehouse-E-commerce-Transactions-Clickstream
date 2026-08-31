from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from gold_utils import build_static_dimension

def build(sessions_df: DataFrame, existing_df: DataFrame | None) -> DataFrame:
    source_values = sessions_df.select(col("country").alias("country_name"))
    return build_static_dimension(source_values, "country_name", "country_key", existing_df)
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from gold_utils import build_scd2_dimension, attach_dimension_key

def build(products_df: DataFrame, dim_category: DataFrame, process_ts: str, existing_df: DataFrame | None) -> DataFrame:
    source = (
        products_df.where(col("product_id").isNotNull())
        .transform(
            lambda df: attach_dimension_key(
                df,
                "category",
                dim_category,
                "category_name",
                "category_key",
                "category_key",
                "product_category"
            )
        ).select(
            "product_id",
            "category_key",
            col("name").alias("product_name"),
            col("price_usd").alias("base_price_usd"),
            "cost_usd"
        )
    )
    
    return build_scd2_dimension(
        source_df=source,
        natural_key="product_id",
        surrogate_key="product_key",
        attribute_cols=[
            "category_key",
            "product_name",
            "base_price_usd",
            "cost_usd",
        ],
        process_ts=process_ts,
        existing_df=existing_df,
    )

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, lit, trim, when
from pyspark.sql.types import StringType


DATASET_PRIMARY_KEYS = {
    "events": "event_id",
    "customers": "customer_id",
    "orders": "order_id",
    "products": "product_id",
    "reviews": "review_id",
    "sessions": "session_id",
    "order_items": None,
}


def normalize_column_names(df: DataFrame) -> DataFrame:
    """Normalize source columns into lower snake_case names."""
    for column in df.columns:
        normalized_column_name = column.lower().strip().replace(" ", "_")
        df = df.withColumnRenamed(column, normalized_column_name)
    return df


def add_canonical_record_id(df: DataFrame, dataset_name: str) -> DataFrame:
    """Add canonical record ID columns to a dataset."""
    if dataset_name == "order_items":
        if "order_id" in df.columns and "product_id" in df.columns:
            return (
                df.withColumn(
                    "record_id",
                    concat_ws(
                        "_",
                        col("order_id").cast(StringType()),
                        col("product_id").cast(StringType()),
                    ),
                )
                .withColumn("record_id_source", lit("order_id_product_id"))
            )
        return (
            df.withColumn("record_id", lit(None).cast(StringType()))
            .withColumn("record_id_source", lit("NOT FOUND"))
        )

    primary_key_col = DATASET_PRIMARY_KEYS.get(dataset_name)
    if primary_key_col and primary_key_col in df.columns:
        return (
            df.withColumn("record_id", col(primary_key_col).cast(StringType()))
            .withColumn("record_id_source", lit(primary_key_col))
        )
    return (
        df.withColumn("record_id", lit(None).cast(StringType()))
        .withColumn("record_id_source", lit("NOT FOUND"))
    )


def add_data_quality_flags(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "error",
            when(col("record_id").isNull(), lit("NULL RECORD ID"))
            .when(trim(col("record_id")) == "", lit("EMPTY RECORD ID"))
            .otherwise(lit(None).cast(StringType())),
        )
        .withColumn("is_valid_record_id", col("error").isNull())
    )

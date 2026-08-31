from pyspark.sql.functions import (
    lit, col, sha2, concat_ws, monotonically_increasing_id, coalesce, broadcast,
    date_format,
    max as spark_max,
)
from pyspark.sql import DataFrame, SparkSession
from utils.logging import create_logger
from path import GOLD_WATERMARK_PATH, GOLD_WATERMARK_TABLE

logger = create_logger(__name__)
TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"

def _ensure_gold_watermark_table(spark: SparkSession):
    """Ensure the Gold watermark Delta table exists and is readable."""
    create_stmt = f"""
        CREATE TABLE IF NOT EXISTS {GOLD_WATERMARK_TABLE}(
            pipeline_name STRING,
            last_process_date DATE,
            updated_at TIMESTAMP
        )
        USING DELTA
        LOCATION '{GOLD_WATERMARK_PATH}'
    """
    
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")
        
    try:
        spark.sql(create_stmt)
        spark.sql(f"REFRESH TABLE {GOLD_WATERMARK_TABLE}")
        spark.table(GOLD_WATERMARK_TABLE).limit(1).collect()
    except Exception as ex:
        logger.warning(f"Recreating broken watermark table metadata: {ex}")
        spark.sql(create_stmt)
        spark.sql(f"REFRESH TABLE {GOLD_WATERMARK_TABLE}")
    
    
def read_silver(spark: SparkSession, dataset: str, process_date: str | None = None) -> DataFrame:
    """Read a Silver Delta dataset, optionally filtered by processed_date."""
    path = f"hdfs://namenode:8020/lakehouse/silver/{dataset}"
    logger.info(f"Reading silver: {path}")
    
    df = spark.read.format("delta").load(path)
    
    if process_date and "processed_date" in df.columns:
        logger.info(f"Applying Silver process_date filter: {process_date}")
        df = df.filter(col("processed_date") == lit(process_date).cast("date"))
    return df


def read_silver_since(spark: SparkSession, dataset: str, watermark: str) -> DataFrame:
    """Read Silver rows with processed_date newer than the watermark."""
    path = f"hdfs://namenode:8020/lakehouse/silver/{dataset}"
    logger.info(f"Reading silver since {watermark}: {path}")
    
    df = spark.read.format("delta").load(path)
    
    if "processed_date" in df.columns:
        return df.filter(col("processed_date") > lit(watermark).cast("date"))
    return df.limit(0)


def delta_table_exist(spark: SparkSession, path: str) -> bool:
    """Return True when a Delta table can be loaded from the path."""
    try:
        spark.read.format("delta").load(path).limit(1).collect()
        return True
    except Exception:
        return False


def read_exists_delta_table(spark: SparkSession, path: str) -> DataFrame | None:
    """Read an existing Delta table or return None when it is missing."""
    if delta_table_exist(spark, path):
        return spark.read.format("delta").load(path)
    return None


def write_gold_overwrite(
    df: DataFrame, 
    spark: SparkSession, 
    table_name: str,
    path: str,
    partition_col: str = None
):
    """Overwrite a Gold Delta table and refresh Hive metadata."""
    full_name = f"gold.{table_name}"
    logger.info(f"Writing Gold (OVERWRITE): {full_name} -> {path}")
    
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")
    
    writer = (
        df.write.mode("overwrite")
        .format("delta")
        .option("path", path)
        .option("overwriteSchema", "true")
    )

    if partition_col and partition_col in df.columns:
        writer = writer.partitionBy(partition_col)
    
    writer.saveAsTable(full_name)
    spark.sql(f"REFRESH TABLE {full_name}")
    

def write_gold_merge(
    df: DataFrame,
    spark: SparkSession,
    table_name: str,
    path: str,
    key_cols: list[str],
    surrogate_col: str | None = None,
):
    """Merge incoming Gold rows by keys while preserving surrogate keys."""
    full_name = f"gold.{table_name}"
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")
    logger.info(f"Writing Gold (MERGE): {full_name} -> {path}")
    
    if not delta_table_exist(spark, path):
        (
            df.write.mode("overwrite") 
            .format("delta") 
            .option("path", path) 
            .saveAsTable(full_name)
        )
        
        spark.sql(f"REFRESH TABLE {full_name}")
        return
    
    prepared_df = df 
    
    if surrogate_col: 
        if surrogate_col not in prepared_df.columns:
            prepared_df = prepared_df.withColumn(surrogate_col, lit(None).cast("long"))
        
        existing_surrogate = (
            spark.read.format("delta").load(path) 
            .select(*key_cols, col(surrogate_col).alias("_existing_surrogate")) 
            .dropDuplicates(key_cols)
        )
        
        prepared_df = df.alias("incoming").join(existing_surrogate.alias("existing"), on=key_cols, how="left")
        
        max_surrogate_row = (
            spark.read.format("delta").load(path)
            .agg(spark_max(surrogate_col).alias("max_surrogate"))
            .first()
        )
        max_surrogate = max_surrogate_row["max_surrogate"] or 0
        
        new_rows = (
            prepared_df.where(col("_existing_surrogate").isNull())
            .withColumn(surrogate_col, monotonically_increasing_id() + lit(max_surrogate + 1))
            .drop("_existing_surrogate")
        )
        
        existing_rows = (
            prepared_df.where(col("_existing_surrogate").isNotNull())
            .withColumn(surrogate_col, col("_existing_surrogate"))
            .drop("_existing_surrogate")
        )
        
        prepared_df = existing_rows.unionByName(new_rows)
    
    merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in key_cols])
    source_view = f"tmp_merge_{table_name}"
    prepared_df.createOrReplaceTempView(source_view)
    update_assignments = ", ".join([f"target.{column} = source.{column}" for column in prepared_df.columns])
    insert_columns = ", ".join(prepared_df.columns)
    insert_values = ", ".join(f"source.{col}" for col in prepared_df.columns)
    
    spark.sql(
        f"""
        MERGE INTO delta.`{path}` AS target
        USING {source_view} AS source
        ON {merge_condition}
        WHEN MATCHED THEN UPDATE SET {update_assignments}
        WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})                  
        """
    )
    spark.sql(f"CREATE TABLE IF NOT EXISTS {full_name} USING DELTA LOCATION '{path}'")
    spark.sql(f"REFRESH TABLE {full_name}")
    
    
def get_latest_silver_process_date(spark: SparkSession, datasets: list[str]) -> str | None:
    """Return the newest processed_date across Silver datasets."""
    latest_value = []
    for dataset in datasets:
        df = read_silver(spark, dataset)
        if "processed_date" not in df.columns:
            continue
        row = df.agg(spark_max("processed_date").alias("max_process_date")).first()
        if row and row["max_process_date"] is not None:
            latest_value.append(str(row["max_process_date"]))
    if not latest_value:
        return None
    return max(latest_value)


def get_gold_watermark(spark: SparkSession, pipeline_name: str):
    """Return the last processed date stored for a Gold pipeline."""
    _ensure_gold_watermark_table(spark)
    row = (
        spark.table(GOLD_WATERMARK_TABLE)
        .where(col("pipeline_name") == lit(pipeline_name))
        .agg(spark_max("last_process_date").alias("last_process_date"))
        .first()
    )
    return None if row is None or row["last_process_date"] is None else str(row["last_process_date"])


def upsert_gold_watermark(spark: SparkSession, pipeline_name: str, process_date: str):
    """Replace the stored Gold watermark for a pipeline."""
    _ensure_gold_watermark_table(spark)
    
    spark.sql(f"DELETE FROM {GOLD_WATERMARK_TABLE} WHERE pipeline_name = '{pipeline_name}'")
    spark.sql(
        f"""
        INSERT INTO {GOLD_WATERMARK_TABLE} (pipeline_name, last_process_date, updated_at)
        SELECT
            '{pipeline_name}',
            to_date('{process_date}'),
            current_timestamp()
        """
    )
    
    
def assign_surrogate_key(
    df: DataFrame,
    key_col: str,
    # priority_cols: list,
    start_at: int = 0,
) -> DataFrame:
    """Add a surrogate key column starting after start_at."""
    return df.withColumn(key_col, monotonically_increasing_id() + lit(start_at + 1))
    
    
def build_static_dimension(
    source_df: DataFrame,
    value_col: str,
    key_col: str,
    existing_df: DataFrame | None = None
) -> DataFrame:
    """Build a static dimension while keeping existing keys stable."""
    source_values = (
        source_df.select(value_col)
        .where(col(value_col).isNotNull())
        .dropDuplicates([value_col])
    )
    
    if existing_df is None:
        return (
            assign_surrogate_key(source_values, key_col)
            .select(key_col, value_col)
        )
    
    existing_values = existing_df.select(key_col, value_col).dropDuplicates([value_col])
    max_key_row = existing_values.agg(spark_max(key_col).alias("max_key")).first()
    max_key = max_key_row["max_key"] or 0
    
    new_values = source_values.join(
        existing_values.select(value_col),
        on=value_col,
        how="left_anti"
    )
    
    new_rows = (
        assign_surrogate_key(new_values, key_col, start_at=max_key)
        .select(key_col, value_col)
    )
    
    return existing_values.unionByName(new_rows)


def _hash_attributes(df: DataFrame, attribute_cols: list[str]) -> DataFrame:
    """Hash SCD2 attribute columns to detect changed records."""
    hash_inputs = [
        coalesce(col(column_name).cast("string"), lit("__NULL__"))
        for column_name in attribute_cols
    ]
    return df.withColumn("_attr_hash", sha2(concat_ws("||", *hash_inputs), 256))


def format_timestamp(column_name_or_value):
    """Format a timestamp column or value for Gold display fields."""
    return date_format(column_name_or_value.cast("timestamp"), TIMESTAMP_FORMAT)
    
    
def build_scd2_dimension(
    source_df: DataFrame,
    natural_key: str,
    surrogate_key: str,
    attribute_cols: list[str],
    process_ts: str,
    existing_df: DataFrame | None = None
) -> DataFrame:
    """Build an SCD Type 2 dimension with current and historical rows."""
    process_ts_col = lit(process_ts).cast("timestamp")
    source = source_df.select(natural_key, *attribute_cols).dropDuplicates([natural_key])
    final_cols = [
        surrogate_key,
        natural_key,
        *attribute_cols,
        "effective_from",
        "effective_to",
        "is_current"
    ]
    
    if existing_df is None:
        return(
            assign_surrogate_key(source, surrogate_key)
            .withColumn("effective_from", format_timestamp(process_ts_col))
            .withColumn("effective_to", lit(None).cast("string"))
            .withColumn("is_current", lit(True))
            .select(*final_cols)
        ) 
        
    existing = (
        existing_df.select(*final_cols)
        .withColumn("effective_from", format_timestamp(col("effective_from")))
        .withColumn("effective_to", format_timestamp(col("effective_to")))
    )
    existing_current = existing.filter(col("is_current") == lit(True))
    existing_history = existing.filter(col("is_current") == lit(False))
    
    source_hashed = _hash_attributes(source, attribute_cols)
    existing_current_hashed = _hash_attributes(existing_current, attribute_cols)
    
    joined = source_hashed.alias("source").join(
        existing_current_hashed.alias("current"),
        on=natural_key,
        how="left"
    )
    
    new_rows = joined.filter(col(f"current.{surrogate_key}").isNull()).select("source.*")
    changed_rows = joined.filter(
        col(f"current.{surrogate_key}").isNotNull() &
        (col("source._attr_hash") != col("current._attr_hash"))
    ).select("source.*")
    
    changed_keys = changed_rows.select(natural_key).dropDuplicates([natural_key])
    
    kept_current = existing_current.join(changed_keys, on=natural_key, how="left-anti")
    expired_current = (
        existing_current.join(changed_keys, on=natural_key, how="inner")
        .withColumn("effective_to", format_timestamp(process_ts_col))
        .withColumn("is_current", lit(False))
    )
    
    max_key_row = existing.agg(spark_max(surrogate_key).alias("max_key")).first()
    max_key = max_key_row["max_key"] or 0
    
    incoming_rows = (
        new_rows.unionByName(changed_rows)
        .drop("_attr_hash")
        .dropDuplicates([natural_key])
    )
    
    new_versions = (
        assign_surrogate_key(incoming_rows, surrogate_key, start_at=max_key)
        .withColumn("effective_from", format_timestamp(process_ts_col))
        .withColumn("effective_to", lit(None).cast("string"))
        .withColumn("is_current", lit(True))
    )
    
    return (
        existing_history.select(*final_cols)
        .unionByName(expired_current.select(*final_cols))
        .unionByName(kept_current.select(*final_cols))
        .unionByName(new_versions.select(*final_cols))
    )


def attach_dimension_key(
    df: DataFrame,
    source_col: str,
    dimension_df: DataFrame,
    dimension_value_col: str,
    dimension_key_col: str,
    output_key_col: str,
    join_alias: str,
) -> DataFrame:
    """Attach a dimension key to a fact DataFrame through a lookup join."""
    lookup_value_col = f"__{join_alias}_{dimension_value_col}"
    lookup_df = broadcast(
        dimension_df.select(
            col(dimension_value_col).alias(lookup_value_col),
            col(dimension_key_col).alias(output_key_col)
        )
    )
    
    return df.join(lookup_df, col(source_col) == col(lookup_value_col), how="left").drop(lookup_value_col)

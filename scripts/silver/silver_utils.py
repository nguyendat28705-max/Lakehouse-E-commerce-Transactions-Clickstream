from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, trim, when, current_timestamp, row_number
from pyspark.sql.window import Window

from utils.logging import create_logger
from delta.tables import DeltaTable


logger = create_logger(__name__)


def read_bronze(spark: SparkSession, dataset: str, ingest_date_filter: str | None = None):
    """Read Bronze data, optionally filtered by ingest_date."""
    path = f"hdfs://namenode:8020/lakehouse/bronze/{dataset}"
    logger.info("Reading Bronze %s", path)
    df = spark.read.format("delta").load(path)
    if ingest_date_filter:
        logger.info("Applying Bronze ingest_date filter: %s", ingest_date_filter)
        df = df.filter(col("ingest_date") == lit(ingest_date_filter))
    return df
        

def filter_valid_record(df: DataFrame) -> DataFrame:
    """Filter records with valid record_id"""
    return df.filter(col("is_valid_record_id") == lit(True))


def safe_trim(df: DataFrame, columns: list) -> DataFrame:
    """Trim strings and convert empty to NULL"""
    for column in columns:
        if column in df.columns:
            df = df.withColumn(
                column,
                when(trim(col(column)) == "", None)
                .otherwise(trim(col(column))))
    return df

def deduplicate(df: DataFrame, key_cols: list, priority_cols: list = None) -> DataFrame:
    """Deduplicate by key, keeping latest record"""
    if priority_cols is None:
        priority_cols = ["ingestion_timestamp"]
        
    priority_cols = [col(column_name).desc() for column_name in priority_cols]
    
    window = Window.partitionBy(*key_cols).orderBy(*priority_cols)
    
    return (
        df.withColumn("_rn", row_number().over(window))
        .filter(col("_rn") ==  1)
        .drop("_rn")
        )
    

def add_silver_metadata(df: DataFrame, process_date: str) -> DataFrame:
    """Add metadata columns to Silver."""
    return (
        df.withColumn("processed_at", current_timestamp())
        .withColumn("processed_date", lit(process_date).cast("date"))
        )
    

def drop_bronze_metadata(df: DataFrame) -> DataFrame:
    """Remove Bronze metadata columns."""
    bronze_metadata = ["record_id", "record_id_source", "error", "is_valid_record_id",
                       "ingest_date", "ingestion_timestamp", "source_file"] 
    return df.drop(*bronze_metadata)


def add_validation_status(df):
    return df.withColumn(
        "status",
        when(col("validation_errors").isNotNull(), "ERROR")
        .when(col("validation_warnings").isNotNull(), "WARNING")
        .otherwise("VALID")
    )


def write_silver_merge(df: DataFrame, spark: SparkSession, silver_path: str, key_cols: list):
    """Upsert data into Silver using Delta MERGE."""

    logger.info(f"Writing Silver with MERGE: {silver_path}")
    
    table_exist = False
    try:
        spark.read.format("delta").load(silver_path).limit(1)
        table_exist = True
    except Exception:
        table_exist = False
        
    if table_exist:
        delta_table = DeltaTable.forPath(spark, silver_path)
        merge_condition = " AND ".join(f"target.{key} = source.{key}" for key in key_cols)
        
        delta_table.alias("target").merge(
            df.alias("source"),
            merge_condition
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
         
        logger.info("Silver MERGE completed")
    else:
        logger.info("Silver table not found, creating new table")
        
        df.write.mode("overwrite").format("delta").save(silver_path)
        
        logger.info("Silver initial write completed")

def write_silver_overwrite(df:DataFrame, path: str, partition_col: str = None):
    """Write data to Silver using overwrite."""
    writer = df.write.mode("overwrite").format("delta")
    if partition_col and partition_col in df.columns:
        writer = writer.partitionBy(partition_col)
    writer.save(path)

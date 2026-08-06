import sys
from datetime import datetime
from bronze.schemas import get_schema_for_dataset
from bronze.transforms import(
    add_canonical_record_id,
    add_data_quality_flags,
    normalize_column_names,
)
from pyspark.sql.functions import current_timestamp, input_file_name, lit
from utils.spark import create_spark_session
from utils.logging import create_logger


logger = create_logger(__name__)

    
def ingest_to_bronze(csv_path: str, hdfs_output_path: str, ingest_date: str, dataset_name: str):
    logger.info(
        "Starting bronze ingestion: dataset=%s, csv_path=%s, ingest_date=%s",
        dataset_name,
        csv_path,
        ingest_date,
    )
    
    spark = None

    try:
        app_name = f"Bronze_Ingestion_{dataset_name}"
        spark = create_spark_session(app_name=app_name)
        logger.info("Spark session created for dataset=%s", dataset_name)

        schema = get_schema_for_dataset(dataset_name)
        if schema:
            logger.info("Reading CSV with predefined schema: dataset=%s", dataset_name)
            df = spark.read.option("header", "true").schema(schema).csv(csv_path)
        else:
            logger.info("Reading CSV with inferred schema: dataset=%s", dataset_name)
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
        logger.info("Loaded CSV: dataset=%s, columns=%s", dataset_name, df.columns)
        
        df = normalize_column_names(df)
        logger.info("Normalized column names: dataset=%s, columns=%s", dataset_name, df.columns)
        
        df = add_canonical_record_id(df, dataset_name)
        logger.info("Added canonical record ID columns: dataset=%s", dataset_name)
        
        df = add_data_quality_flags(df)
        logger.info("Added data quality flags: dataset=%s", dataset_name)
        
        df = df.withColumn("ingest_date", lit(ingest_date))\
            .withColumn("ingestion_timestamp", current_timestamp())\
            .withColumn("source_file", input_file_name())
            
        out_full_path = f"{hdfs_output_path}/{dataset_name}"
        logger.info("Writing Delta table: dataset=%s, output_path=%s", dataset_name, out_full_path)
        
        df.write.mode("append").partitionBy("ingest_date").format("delta").save(out_full_path)
        logger.info("Finished bronze ingestion: dataset=%s, output_path=%s", dataset_name, out_full_path)
    except Exception:
        logger.exception(
            "Bronze ingestion failed: dataset=%s, csv_path=%s, output_path=%s",
            dataset_name,
            csv_path,
            hdfs_output_path,
        )
        raise
    finally:
        if spark is not None:
            logger.info("Stopping Spark session: dataset=%s", dataset_name)
            spark.stop()

def main():
    if len(sys.argv) > 1:
        ingest_date = sys.argv[1]
    else:
        ingest_date = datetime.now().strftime("%Y-%m-%d")
    
    if len(sys.argv) > 2:
        file_name = sys.argv[2]
    else:
        file_name = "events.csv"
        
    if len(sys.argv) > 3:
        dataset_name = sys.argv[3]
    else:
        dataset_name = file_name.replace(".csv", "")
    
    CSV_INPUT_PATH = f"file:///opt/project/data/raw/{file_name}"
    HDFS_OUTPUT_BASE = "hdfs://namenode:8020/lakehouse/bronze"
    
    ingest_to_bronze(
        csv_path=CSV_INPUT_PATH, 
        hdfs_output_path=HDFS_OUTPUT_BASE,
        ingest_date=ingest_date,
        dataset_name=dataset_name
    )

if __name__ == "__main__":
    main()
    
        

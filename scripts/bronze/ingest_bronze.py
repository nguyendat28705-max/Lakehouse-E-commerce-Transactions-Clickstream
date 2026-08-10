import argparse
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

from bronze.schemas import get_schema_for_dataset
from bronze.transforms import (
    add_canonical_record_id,
    add_data_quality_flags,
    normalize_column_names,
)
from pyspark.sql.functions import current_timestamp, input_file_name, lit
from utils.spark import create_spark_session
from utils.logging import create_logger


logger = create_logger(__name__)


CSV_INPUT_BASE = "file:///opt/project/data/raw"
HDFS_OUTPUT_BASE = "hdfs://namenode:8020/lakehouse/bronze"


def validate_ingest_date(ingest_date: str) -> str:
    try:
        return datetime.strptime(ingest_date, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("ingest_date must use YYYY-MM-DD format") from exc


def validate_file_name(file_name: str) -> str:
    if not file_name or "/" in file_name or "\\" in file_name or file_name in {".", ".."}:
        raise ValueError("file_name must be a CSV file name, not a path")
    if not file_name.endswith(".csv"):
        raise ValueError("file_name must end with .csv")
    return file_name


def build_csv_path(input_base: str, file_name: str) -> str:
    return f"{input_base.rstrip('/')}/{validate_file_name(file_name)}"


def validate_local_input_exists(csv_path: str) -> None:
    parsed = urlparse(csv_path)
    if parsed.scheme != "file":
        return

    local_path = Path(parsed.path)
    if not local_path.exists():
        raise FileNotFoundError(f"Input CSV does not exist: {csv_path}")


def ingest_to_bronze(csv_path: str, hdfs_output_path: str, ingest_date: str, dataset_name: str):
    ingest_date = validate_ingest_date(ingest_date)
    validate_local_input_exists(csv_path)

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
        if schema is None:
            raise ValueError(f"Unsupported dataset_name: {dataset_name}")

        logger.info("Reading CSV with predefined schema: dataset=%s", dataset_name)
        df = spark.read.option("header", "true").schema(schema).csv(csv_path)
        logger.info("Loaded CSV: dataset=%s, columns=%s", dataset_name, df.columns)

        df = normalize_column_names(df)
        logger.info("Normalized column names: dataset=%s, columns=%s", dataset_name, df.columns)

        df = add_canonical_record_id(df, dataset_name)
        logger.info("Added canonical record ID columns: dataset=%s", dataset_name)

        df = add_data_quality_flags(df)
        logger.info("Added data quality flags: dataset=%s", dataset_name)

        df = df.withColumn("ingest_date", lit(ingest_date)) \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("source_file", input_file_name())

        out_full_path = f"{hdfs_output_path}/{dataset_name}"
        replace_where = f"ingest_date = '{ingest_date}'"
        logger.info(
            "Writing Delta table partition: dataset=%s, output_path=%s, replace_where=%s",
            dataset_name,
            out_full_path,
            replace_where,
        )

        (
            df.write
            .mode("overwrite")
            .option("replaceWhere", replace_where)
            .partitionBy("ingest_date")
            .format("delta")
            .save(out_full_path)
        )
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


def parse_args():
    parser = argparse.ArgumentParser(description="Ingest a raw CSV file into a Bronze Delta table.")
    parser.add_argument("ingest_date", nargs="?", default=datetime.now().strftime("%Y-%m-%d"))
    parser.add_argument("file_name", nargs="?", default="events.csv")
    parser.add_argument("dataset_name", nargs="?")
    return parser.parse_args()


def main():
    args = parse_args()
    ingest_date = validate_ingest_date(args.ingest_date)
    file_name = validate_file_name(args.file_name)
    dataset_name = args.dataset_name or file_name.replace(".csv", "")

    csv_input_path = build_csv_path(CSV_INPUT_BASE, file_name)

    ingest_to_bronze(
        csv_path=csv_input_path,
        hdfs_output_path=HDFS_OUTPUT_BASE,
        ingest_date=ingest_date,
        dataset_name=dataset_name,
    )

if __name__ == "__main__":
    main()

import argparse
from datetime import datetime

from utils.spark import create_spark_session
from utils.logging import create_logger
from silver_utils import (write_silver_merge, write_silver_overwrite, drop_bronze_metadata, add_silver_metadata,
                          filter_valid_record, read_bronze, deduplicate, add_validation_status)
from clean_customers import clean_customers_df
from clean_events import clean_events_df
from clean_order_items import clean_order_items_df
from clean_orders import clean_orders_df
from clean_products import clean_products_df
from clean_reviews import clean_reviews_df
from clean_sessions import clean_sessions_df


logger = create_logger(__name__)

SILVER_BASE_PATH = "hdfs://namenode:8020/lakehouse/silver"

DATASET_KEYS = {
    "events": ["event_id"],
    "customers": ["customer_id"],
    "orders": ["order_id"],
    "products": ["product_id"],
    "reviews": ["review_id"],
    "sessions": ["session_id"],
    "order_items": ["order_id", "product_id"],
}

DATASET_CLEANERS = {
    "events": clean_events_df,
    "customers": clean_customers_df,
    "orders": clean_orders_df,
    "products": clean_products_df,
    "reviews": clean_reviews_df,
    "sessions": clean_sessions_df,
    "order_items": clean_order_items_df,
}

DATASET_PRIORITY_COLS = {
    "events": ["timestamp", "ingestion_timestamp"],
    "orders": ["order_time", "ingestion_timestamp"],
    "sessions": ["start_time", "ingestion_timestamp"],
    "reviews": ["review_time", "ingestion_timestamp"],
}

FULL_REBUILD_OVERWRITE_DATASETS = {"customers", "products"}


def validate_process_date(process_date: str) -> str:
    try:
        return datetime.strptime(process_date, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("process_date must use YYYY-MM-DD format") from exc


def validate_dataset_name(dataset_name: str) -> str:
    if dataset_name not in DATASET_CLEANERS:
        supported = ", ".join(sorted(DATASET_CLEANERS))
        raise ValueError(f"Unsupported dataset_name: {dataset_name}. Supported datasets: {supported}")
    return dataset_name


def clean_silver(process_date: str, dataset_name: str) -> None:
    process_date = validate_process_date(process_date)
    dataset_name = validate_dataset_name(dataset_name)

    logger.info("Starting Silver cleaning: dataset=%s, process_date=%s", dataset_name, process_date)
    spark = create_spark_session(f"silver_{dataset_name}")
        
    silver_path = f"{SILVER_BASE_PATH}/{dataset_name}"
    
    try:
        key_cols = DATASET_KEYS[dataset_name]
        cleaner = DATASET_CLEANERS[dataset_name]

        df = read_bronze(spark, dataset_name, process_date)
        df = filter_valid_record(df)
        df = cleaner(df, process_date)
        df = deduplicate(df, key_cols, DATASET_PRIORITY_COLS.get(dataset_name))
        df = add_silver_metadata(df, process_date)
        df = drop_bronze_metadata(df)
        df = add_validation_status(df)
        
        if dataset_name in FULL_REBUILD_OVERWRITE_DATASETS:
            write_silver_overwrite(df, silver_path)
        else:
            write_silver_merge(df, spark, silver_path, key_cols)
        logger.info("Finished Silver cleaning: dataset=%s, silver_path=%s", dataset_name, silver_path)
    finally:
        spark.stop()
     
     
def parse_args():
    parser = argparse.ArgumentParser(description="Clean a Bronze Delta dataset into Silver.")
    parser.add_argument("process_date")
    parser.add_argument("dataset_name", choices=sorted(DATASET_CLEANERS))
    return parser.parse_args()

     
def main():
    args = parse_args()
    clean_silver(args.process_date, args.dataset_name)
        
if __name__ == "__main__":
    main()
    

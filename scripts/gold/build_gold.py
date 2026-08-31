import argparse
from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit

from gold import (
    dim_category,
    dim_country,
    dim_customer,
    dim_date,
    dim_device,
    dim_payment_method,
    dim_product,
    dim_source,
    fact_customer_funnel,
    fact_order,
    fact_order_item,
    fact_review,
    fact_session,
    fact_web_event,
)
from gold_utils import (
    get_gold_watermark,
    get_latest_silver_process_date,
    read_exists_delta_table,
    read_silver as read_silver_base,
    read_silver_since,
    upsert_gold_watermark,
    write_gold_merge,
    write_gold_overwrite,
)
from path import GOLD_PATHS
from utils.logging import create_logger
from utils.spark import create_spark_session


SILVER_DATASETS = [
    "customers",
    "orders",
    "order_items",
    "products",
    "reviews",
    "sessions",
    "events",
]
PIPELINE_NAME = "gold_star_schema"

DIMENSION_TABLES = {
    "dim_country",
    "dim_source",
    "dim_device",
    "dim_payment_method",
    "dim_category",
    "dim_date",
    "dim_customer",
    "dim_product",
}
FACT_TABLES = {
    "fact_order",
    "fact_order_item",
    "fact_web_event",
    "fact_review",
    "fact_session",
    "fact_customer_funnel",
}
SUPPORTED_TABLES = DIMENSION_TABLES | FACT_TABLES | {"update_watermark"}

DATE_ALIASES = {
    "orders": ("order_date", "order_time"),
    "reviews": ("review_date", "review_time"),
    "sessions": ("session_date", "start_time"),
    "events": ("event_date", "timestamp"),
}

INCREMENTAL_FACT_SPECS = {
    "fact_order": (["order_id"], "order_key"),
    "fact_order_item": (["order_id", "product_key"], "order_item_key"),
    "fact_web_event": (["event_id"], "event_key"),
    "fact_review": (["review_id"], "review_key"),
    "fact_session": (["session_id"], "session_key"),
}

FACT_DIM_REQUIREMENTS = {
    "fact_order": [
        "dim_date",
        "dim_country",
        "dim_source",
        "dim_device",
        "dim_payment_method",
        "dim_customer",
        "dim_product",
    ],
    "fact_order_item": ["dim_date", "dim_customer", "dim_product"],
    "fact_web_event": ["dim_date", "dim_customer", "dim_product"],
    "fact_review": ["dim_product"],
    "fact_session": ["dim_date", "dim_country", "dim_source", "dim_device", "dim_customer"],
    "fact_customer_funnel": ["dim_date", "dim_customer", "dim_product"],
}

logger = create_logger(__name__)


@dataclass
class BuildState:
    """Store whether a Gold table should full load, increment, or skip."""
    full_load: bool
    incremental_watermark: str | None = None
    should_skip: bool = False


@dataclass
class SilverFrames:
    """Group all Silver DataFrames needed by Gold builders."""
    customers: DataFrame
    orders: DataFrame
    order_items: DataFrame
    products: DataFrame
    reviews: DataFrame
    sessions: DataFrame
    events: DataFrame


@dataclass
class ValidSilverFrames:
    """Group valid Silver DataFrames used by fact builders."""
    orders: DataFrame
    order_items: DataFrame
    reviews: DataFrame
    sessions: DataFrame
    events: DataFrame
    events_with_customer: DataFrame


def _validate_process_date(process_date: str) -> str:
    """Validate and normalize a process date string."""
    try:
        return datetime.strptime(process_date, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("process_date must use YYYY-MM-DD format") from exc


def _validate_table_name(table_name: str) -> str:
    """Validate that the requested Gold table is supported."""
    if table_name not in SUPPORTED_TABLES:
        supported = ", ".join(sorted(SUPPORTED_TABLES))
        raise ValueError(f"Unsupported table '{table_name}'. Supported tables: {supported}")
    return table_name


def _write_gold_full(df: DataFrame, spark: SparkSession, table_name: str) -> None:
    """Write a Gold table with full overwrite semantics."""
    write_gold_overwrite(df, spark, table_name, GOLD_PATHS[table_name])


def _write_gold_fact_incremental(
    df: DataFrame,
    spark: SparkSession,
    table_name: str,
    key_cols: list[str],
    surrogate_col: str,
) -> None:
    """Write an incremental fact table using Delta merge."""
    write_gold_merge(
        df=df,
        spark=spark,
        table_name=table_name,
        path=GOLD_PATHS[table_name],
        key_cols=key_cols,
        surrogate_col=surrogate_col,
    )


def _write_incremental_fact(df: DataFrame, spark: SparkSession, table_name: str) -> None:
    """Resolve merge keys for a fact table and write it incrementally."""
    key_cols, surrogate_col = INCREMENTAL_FACT_SPECS[table_name]
    _write_gold_fact_incremental(df, spark, table_name, key_cols, surrogate_col)


def _read_gold_required(spark: SparkSession, table_name: str) -> DataFrame:
    """Read a required Gold dependency or fail with a clear error."""
    df = read_exists_delta_table(spark, GOLD_PATHS[table_name])
    if df is None:
        raise RuntimeError(f"Missing dependency gold.{table_name}. Run prerequisite Gold tasks first.")
    return df


def _read_gold_existing(spark: SparkSession, table_name: str) -> DataFrame | None:
    """Read an existing Gold table when it has already been created."""
    return read_exists_delta_table(spark, GOLD_PATHS[table_name])


def _is_empty(df: DataFrame) -> bool:
    """Return True when a DataFrame has no rows."""
    return df.limit(1).count() == 0


def _gold_table_has_data(spark: SparkSession, table_name: str) -> bool:
    """Return True when a Gold table exists and contains rows."""
    existing_df = _read_gold_existing(spark, table_name)
    return existing_df is not None and not _is_empty(existing_df)


def _add_date_alias(dataset: str, df: DataFrame) -> DataFrame:
    """Add date-only aliases from timestamp columns when needed."""
    alias_config = DATE_ALIASES.get(dataset)
    if alias_config is None:
        return df

    output_col, source_col = alias_config
    if output_col in df.columns or source_col not in df.columns:
        return df
    return df.withColumn(output_col, col(source_col).cast("date"))


def _read_silver_dataset(spark: SparkSession, dataset: str) -> DataFrame:
    """Read a Silver dataset and add its Gold date alias."""
    return _add_date_alias(dataset, read_silver_base(spark, dataset))


def _read_silver_delta(spark: SparkSession, dataset: str, watermark: str) -> DataFrame:
    """Read valid Silver rows newer than the Gold watermark."""
    return _valid_records(_add_date_alias(dataset, read_silver_since(spark, dataset, watermark)))


def _load_silver_frames(spark: SparkSession) -> SilverFrames:
    """Load all Silver datasets used by the Gold layer."""
    return SilverFrames(
        customers=_read_silver_dataset(spark, "customers"),
        orders=_read_silver_dataset(spark, "orders"),
        order_items=_read_silver_dataset(spark, "order_items"),
        products=_read_silver_dataset(spark, "products"),
        reviews=_read_silver_dataset(spark, "reviews"),
        sessions=_read_silver_dataset(spark, "sessions"),
        events=_read_silver_dataset(spark, "events"),
    )


def _valid_records(df: DataFrame) -> DataFrame:
    """Filter out Silver rows that failed validation."""
    if "validation_errors" not in df.columns:
        return df
    return df.where(col("validation_errors").isNull())


def _load_valid_silver_frames(silver: SilverFrames) -> ValidSilverFrames:
    """Prepare valid Silver frames and attach customers to events."""
    valid_orders = _valid_records(silver.orders)
    valid_order_items = _valid_records(silver.order_items)
    valid_reviews = _valid_records(silver.reviews)
    valid_sessions = _valid_records(silver.sessions)
    valid_events = _valid_records(silver.events)
    events_with_customer = valid_events.join(
        valid_sessions.select("session_id", "customer_id"),
        on="session_id",
        how="inner",
    )

    return ValidSilverFrames(
        orders=valid_orders,
        order_items=valid_order_items,
        reviews=valid_reviews,
        sessions=valid_sessions,
        events=valid_events,
        events_with_customer=events_with_customer,
    )


def _load_fact_dimensions(spark: SparkSession, table_name: str) -> dict[str, DataFrame]:
    """Load dimension tables required by a fact table."""
    dims = {
        dim_name: _read_gold_required(spark, dim_name)
        for dim_name in FACT_DIM_REQUIREMENTS[table_name]
    }

    if "dim_customer" in dims:
        dims["current_customers"] = dims["dim_customer"].where(col("is_current") == lit(True))
    if "dim_product" in dims:
        dims["current_products"] = dims["dim_product"].where(col("is_current") == lit(True))
    return dims


def _update_watermark(spark: SparkSession) -> None:
    """Update the Gold watermark from the latest Silver processed date."""
    latest_processed = get_latest_silver_process_date(spark, SILVER_DATASETS)
    if latest_processed is None:
        logger.info("No Silver watermark found; skip watermark update.")
        return

    upsert_gold_watermark(spark, PIPELINE_NAME, latest_processed)
    logger.info(f"Gold watermark updated to {latest_processed}")


def _resolve_build_state(spark: SparkSession, table_name: str) -> BuildState:
    """Decide whether a table should full load, increment, or skip."""
    if not _gold_table_has_data(spark, table_name):
        logger.info(f"Gold table {table_name} has no data; running full load.")
        return BuildState(full_load=True)

    latest_silver_process_date = get_latest_silver_process_date(spark, SILVER_DATASETS)
    last_gold_watermark = get_gold_watermark(spark, PIPELINE_NAME)

    if latest_silver_process_date is None:
        logger.info("No Silver process watermark found; skipping table build.")
        return BuildState(full_load=False, should_skip=True)

    if last_gold_watermark is None:
        logger.info("Gold watermark not found; running full load.")
        return BuildState(full_load=True)

    if latest_silver_process_date <= last_gold_watermark:
        logger.info(
            "No new Silver watermark detected "
            f"(latest={latest_silver_process_date}, gold={last_gold_watermark}); skipping table build."
        )
        return BuildState(full_load=False, should_skip=True)

    return BuildState(full_load=False, incremental_watermark=last_gold_watermark)


def _should_run_incremental_fact(table_name: str, state: BuildState) -> bool:
    """Return True when a fact table can run incremental merge."""
    return (
        table_name in INCREMENTAL_FACT_SPECS
        and not state.full_load
        and state.incremental_watermark is not None
    )


def _build_dimension_table(
    spark: SparkSession,
    table_name: str,
    silver: SilverFrames,
    process_date: str,
    process_ts: str,
) -> DataFrame:
    """Build one Gold dimension table by dispatching to its module."""
    existing_df = _read_gold_existing(spark, table_name)

    if table_name == "dim_country":
        return dim_country.build(silver.sessions, existing_df)

    if table_name == "dim_source":
        return dim_source.build(silver.sessions, existing_df)

    if table_name == "dim_device":
        return dim_device.build(silver.sessions, existing_df)

    if table_name == "dim_payment_method":
        return dim_payment_method.build(silver.orders, existing_df)

    if table_name == "dim_category":
        return dim_category.build(silver.products, existing_df)

    if table_name == "dim_date":
        return dim_date.build(spark)

    if table_name == "dim_customer":
        return dim_customer.build(silver.customers, process_date, process_ts, existing_df)

    if table_name == "dim_product":
        dim_category_df = _read_gold_required(spark, "dim_category")
        return dim_product.build(silver.products, dim_category_df, process_ts, existing_df)

    raise ValueError(f"Unsupported dimension table: {table_name}")


def _build_full_fact_table(
    spark: SparkSession,
    table_name: str,
    valid: ValidSilverFrames,
) -> DataFrame:
    """Build one full fact table with its required dimensions."""
    dims = _load_fact_dimensions(spark, table_name)

    if table_name == "fact_order":
        return fact_order.build(
            valid.orders,
            valid.order_items,
            dims["dim_date"],
            dims["dim_country"],
            dims["dim_source"],
            dims["dim_device"],
            dims["dim_payment_method"],
            dims["current_customers"],
            dims["current_products"],
        )

    if table_name == "fact_order_item":
        return fact_order_item.build(
            valid.orders,
            valid.order_items,
            dims["dim_date"],
            dims["current_customers"],
            dims["current_products"],
        )

    if table_name == "fact_web_event":
        return fact_web_event.build(
            valid.events_with_customer,
            dims["dim_date"],
            dims["current_customers"],
            dims["current_products"],
        )

    if table_name == "fact_review":
        return fact_review.build(valid.reviews, dims["current_products"])

    if table_name == "fact_session":
        return fact_session.build(
            valid.sessions,
            dims["dim_date"],
            dims["dim_country"],
            dims["dim_source"],
            dims["dim_device"],
            dims["current_customers"],
        )

    if table_name == "fact_customer_funnel":
        return fact_customer_funnel.build(
            valid.events_with_customer,
            valid.orders,
            valid.order_items,
            dims["dim_date"],
            dims["current_customers"],
            dims["current_products"],
        )

    raise ValueError(f"Unsupported fact table: {table_name}")


def _build_incremental_fact_order(
    spark: SparkSession,
    valid: ValidSilverFrames,
    watermark: str,
) -> DataFrame | None:
    """Build changed order facts from new or updated Silver orders."""
    orders_delta = _read_silver_delta(spark, "orders", watermark)
    if _is_empty(orders_delta):
        logger.info("No delta rows for fact_order; skipping.")
        return None

    delta_order_ids = orders_delta.select("order_id").dropDuplicates(["order_id"])
    order_items_delta_related = valid.order_items.join(delta_order_ids, on="order_id", how="inner")
    if _is_empty(order_items_delta_related):
        logger.info("No related order_items for fact_order delta; skipping.")
        return None

    dims = _load_fact_dimensions(spark, "fact_order")
    return fact_order.build(
        orders_delta,
        order_items_delta_related,
        dims["dim_date"],
        dims["dim_country"],
        dims["dim_source"],
        dims["dim_device"],
        dims["dim_payment_method"],
        dims["current_customers"],
        dims["current_products"],
    )


def _build_incremental_fact_order_item(
    spark: SparkSession,
    valid: ValidSilverFrames,
    watermark: str,
) -> DataFrame | None:
    """Build changed order item facts from new Silver order items."""
    order_items_delta = _read_silver_delta(spark, "order_items", watermark)
    if _is_empty(order_items_delta):
        logger.info("No delta rows for fact_order_item; skipping.")
        return None

    dims = _load_fact_dimensions(spark, "fact_order_item")
    return fact_order_item.build(
        valid.orders,
        order_items_delta,
        dims["dim_date"],
        dims["current_customers"],
        dims["current_products"],
    )


def _build_incremental_fact_web_event(
    spark: SparkSession,
    valid: ValidSilverFrames,
    watermark: str,
) -> DataFrame | None:
    """Build changed web event facts from new Silver events."""
    events_delta = _read_silver_delta(spark, "events", watermark)
    if _is_empty(events_delta):
        logger.info("No delta rows for fact_web_event; skipping.")
        return None

    delta_session_ids = events_delta.select("session_id").dropDuplicates(["session_id"])
    sessions_related = valid.sessions.join(delta_session_ids, on="session_id", how="inner")
    events_delta_with_customer = events_delta.join(
        sessions_related.select("session_id", "customer_id"),
        on="session_id",
        how="inner",
    )
    if _is_empty(events_delta_with_customer):
        logger.info("No related sessions for fact_web_event delta; skipping.")
        return None

    dims = _load_fact_dimensions(spark, "fact_web_event")
    return fact_web_event.build(
        events_delta_with_customer,
        dims["dim_date"],
        dims["current_customers"],
        dims["current_products"],
    )


def _build_incremental_fact_review(
    spark: SparkSession,
    watermark: str,
) -> DataFrame | None:
    """Build changed review facts from new Silver reviews."""
    reviews_delta = _read_silver_delta(spark, "reviews", watermark)
    if _is_empty(reviews_delta):
        logger.info("No delta rows for fact_review; skipping.")
        return None

    dims = _load_fact_dimensions(spark, "fact_review")
    return fact_review.build(reviews_delta, dims["current_products"])


def _build_incremental_fact_session(
    spark: SparkSession,
    watermark: str,
) -> DataFrame | None:
    """Build changed session facts from new Silver sessions."""
    sessions_delta = _read_silver_delta(spark, "sessions", watermark)
    if _is_empty(sessions_delta):
        logger.info("No delta rows for fact_session; skipping.")
        return None

    dims = _load_fact_dimensions(spark, "fact_session")
    return fact_session.build(
        sessions_delta,
        dims["dim_date"],
        dims["dim_country"],
        dims["dim_source"],
        dims["dim_device"],
        dims["current_customers"],
    )


def _build_incremental_fact_table(
    spark: SparkSession,
    table_name: str,
    valid: ValidSilverFrames,
    watermark: str,
) -> DataFrame | None:
    """Dispatch incremental fact building by table name."""
    if table_name == "fact_order":
        return _build_incremental_fact_order(spark, valid, watermark)

    if table_name == "fact_order_item":
        return _build_incremental_fact_order_item(spark, valid, watermark)

    if table_name == "fact_web_event":
        return _build_incremental_fact_web_event(spark, valid, watermark)

    if table_name == "fact_review":
        return _build_incremental_fact_review(spark, watermark)

    if table_name == "fact_session":
        return _build_incremental_fact_session(spark, watermark)

    raise ValueError(f"Unsupported incremental fact table: {table_name}")


def build_gold(process_date: str, table_name: str) -> None:
    """Build one Gold dimension, fact, or watermark task."""
    process_date = _validate_process_date(process_date)
    table_name = _validate_table_name(table_name)

    spark = create_spark_session(f"Gold_{table_name}")
    process_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    try:
        if table_name == "update_watermark":
            _update_watermark(spark)
            return

        state = _resolve_build_state(spark, table_name)
        if state.should_skip:
            return

        silver = _load_silver_frames(spark)

        if table_name in DIMENSION_TABLES:
            df = _build_dimension_table(spark, table_name, silver, process_date, process_ts)
            _write_gold_full(df, spark, table_name)
            return

        valid = _load_valid_silver_frames(silver)

        if _should_run_incremental_fact(table_name, state):
            df = _build_incremental_fact_table(spark, table_name, valid, state.incremental_watermark)
            if df is None:
                return

            _write_incremental_fact(df, spark, table_name)
            return

        df = _build_full_fact_table(spark, table_name, valid)
        _write_gold_full(df, spark, table_name)
    finally:
        spark.stop()


def parse_args():
    """Parse command-line arguments for the Gold build script."""
    parser = argparse.ArgumentParser(description="Build one Gold dimension or fact table.")
    parser.add_argument("process_date")
    parser.add_argument("table_name", choices=sorted(SUPPORTED_TABLES))
    return parser.parse_args()


def main():
    """Run the Gold build script from the command line."""
    args = parse_args()
    logger.info(f"=== GOLD SINGLE TABLE BUILD: {args.table_name} ===")
    build_gold(args.process_date, args.table_name)
    logger.info("=== GOLD SINGLE TABLE BUILD COMPLETED ===")

if __name__ == "__main__":
    main()

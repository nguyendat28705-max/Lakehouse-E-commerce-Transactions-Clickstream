GOLD_BASE_PATH = "hdfs://namenode:8020/lakehouse/gold"
GOLD_WATERMARK_TABLE = "gold.pipeline_watermark"
GOLD_WATERMARK_PATH = f"{GOLD_BASE_PATH}/pipeline_watermark"

GOLD_PATHS = {
    "dim_country": f"{GOLD_BASE_PATH}/dim_country",
    "dim_source": f"{GOLD_BASE_PATH}/dim_source",
    "dim_device": f"{GOLD_BASE_PATH}/dim_device",
    "dim_payment_method": f"{GOLD_BASE_PATH}/dim_payment_method",
    "dim_category": f"{GOLD_BASE_PATH}/dim_category",
    "dim_date": f"{GOLD_BASE_PATH}/dim_date",
    "dim_customer": f"{GOLD_BASE_PATH}/dim_customer",
    "dim_product": f"{GOLD_BASE_PATH}/dim_product",
    "fact_order": f"{GOLD_BASE_PATH}/fact_order",
    "fact_order_item": f"{GOLD_BASE_PATH}/fact_order_item",
    "fact_web_event": f"{GOLD_BASE_PATH}/fact_web_event",
    "fact_review": f"{GOLD_BASE_PATH}/fact_review",
    "fact_session": f"{GOLD_BASE_PATH}/fact_session",
    "fact_customer_funnel": f"{GOLD_BASE_PATH}/fact_customer_funnel",
}

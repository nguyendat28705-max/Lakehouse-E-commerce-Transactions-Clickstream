from pyspark.sql import DataFrame

from gold_utils import assign_surrogate_key


def build(
    reviews_df: DataFrame,
    current_products: DataFrame,
) -> DataFrame:
    fact = (
        reviews_df.join(current_products.select("product_id", "product_key"), on="product_id", how="left")
        .select(
            "review_id",
            "order_id",
            "product_key",
            "rating",
            "review_text",
            "review_date",
        )
    )

    return (
        assign_surrogate_key(fact, "review_key")
        .select("review_key", "review_id", "order_id", "product_key", "rating", "review_text", "review_date")
    )


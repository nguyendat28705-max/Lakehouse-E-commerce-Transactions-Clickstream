from pyspark.sql.functions import(
    col, when, lower, to_date, year, split, concat_ws, lit
)
from pyspark.sql.types import BooleanType
from silver_utils import safe_trim
from pyspark.sql import DataFrame


def clean_customers_df(df: DataFrame, process_date: str) -> DataFrame:
    df = df.withColumn("signup_date", to_date(col("signup_date")))
        
    df = safe_trim(df, ["name", "email", "country", "marketing_opt_in"])
        
    df = df.withColumn("marketing_opt_in",
        when(lower(col("marketing_opt_in")).isin("true", "1", "yes"), True)
        .when(lower(col("marketing_opt_in")).isin("false", "0", "no"), False)
        .otherwise(None).cast(BooleanType())
        )
        
    process_date_col = lit(process_date).cast("date")
        
    df = df.withColumn("validation_errors", concat_ws(",",
        when(col("customer_id").isNull(), "NULL_CUSTOMER_ID"),
        when((col("customer_id").isNotNull()) & (col("customer_id") <= 0), "INVALID_CUSTOMER_ID"),
        
        when(col("email").isNull(), "NULL_EMAIL"),
        when(~col("email").rlike("^[A-Za-z0-9+_.-]+@(.+)$"), "INVALID_EMAIL_FORMAT"),
        
        when(col("signup_date").isNull(), "NULL_SIGNUP_DATE"),
        when(col("signup_date") > process_date_col, "FUTURE_SIGNUP_DATE"),
        
        when(col("age").isNull(), "NULL_AGE"),
        when((col("age") < 0) | (col("age") > 150), "INVALID_AGE"),
        ))
        
    df = df.withColumn("validation_errors", when(col("validation_errors") == "", None).otherwise(col("validation_errors")))
    
    df = df.withColumn("validation_warnings", concat_ws(",",
        when(col("name").isNull(), "NULL_NAME"),
        when(col("country").isNull(), "NULL_COUNTRY"),
        when(col("marketing_opt_in").isNull(), "NULL_MARKETING_OPT_IN")
    ))

    df = df.withColumn("validation_warnings", when(col("validation_warnings") == "", None).otherwise(col("validation_warnings")))
    
    df = df.withColumn("name", when(col("name").isNull(), lit("Unknown")).otherwise(col("name")))

    return df 

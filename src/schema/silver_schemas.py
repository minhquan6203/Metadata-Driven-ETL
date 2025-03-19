"""
Schema definitions for silver layer tables.
These schemas define the expected structure of curated data in the silver layer.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
    DateType,
    BooleanType,
)


# Sales clean schema
sales_clean_schema = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("transaction_date", DateType(), False),
        StructField("amount", DoubleType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("store_id", StringType(), True),
        # Optional data quality flag columns
        StructField("dq_valid_customer_flag", StringType(), True),
        # Metadata columns
        StructField("silver_insert_timestamp", TimestampType(), False),
    ]
)

# Customers schema
customers_schema = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zip_code", StringType(), True),
        StructField("registration_date", DateType(), True),
        # Optional data quality flag columns
        StructField("dq_valid_email_flag", StringType(), True),
        # Metadata columns
        StructField("silver_insert_timestamp", TimestampType(), False),
    ]
)

# Products schema
products_schema = StructType(
    [
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), False),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), False),
        StructField("cost", DoubleType(), True),
        StructField("supplier_id", StringType(), True),
        # Optional data quality flag columns
        StructField("dq_price_check_flag", StringType(), True),
        # Metadata columns
        StructField("silver_insert_timestamp", TimestampType(), False),
    ]
)

# Data quality metrics schema
data_quality_metrics_schema = StructType(
    [
        StructField("run_timestamp", TimestampType(), False),
        StructField("rule_name", StringType(), False),
        StructField("rule_type", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("column_name", StringType(), True),
        StructField("total_records", IntegerType(), False),
        StructField("valid_records", IntegerType(), False),
        StructField("invalid_records", IntegerType(), False),
        StructField("is_valid", BooleanType(), False),
        StructField("run_id", StringType(), False),
    ]
)

# Schema registry for easy lookup
silver_schema_registry = {
    "silver.sales_clean": sales_clean_schema,
    "silver.customers": customers_schema,
    "silver.products": products_schema,
    "metadata.data_quality_metrics": data_quality_metrics_schema,
}

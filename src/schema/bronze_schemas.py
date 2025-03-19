"""
Schema definitions for bronze layer tables.
These schemas define the expected structure of raw data in the bronze layer.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
    DateType,
)


# Sales transactions schema
sales_transactions_schema = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("transaction_date", DateType(), False),
        StructField("amount", DoubleType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("store_id", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("transaction_time", TimestampType(), True),
        # Metadata columns
        StructField("ingestion_timestamp", TimestampType(), False),
        StructField("source_id", StringType(), False),
    ]
)

# Customer schema
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
        # Metadata columns
        StructField("ingestion_timestamp", TimestampType(), False),
        StructField("source_id", StringType(), False),
    ]
)

# Product catalog schema
products_schema = StructType(
    [
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), False),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), False),
        StructField("cost", DoubleType(), True),
        StructField("supplier_id", StringType(), True),
        StructField("description", StringType(), True),
        # Metadata columns
        StructField("ingestion_timestamp", TimestampType(), False),
        StructField("source_id", StringType(), False),
    ]
)

# Schema registry for easy lookup
bronze_schema_registry = {
    "bronze.sales_transactions": sales_transactions_schema,
    "bronze.customers": customers_schema,
    "bronze.products": products_schema,
}

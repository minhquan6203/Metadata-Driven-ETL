"""
Schema definitions for gold layer tables.
These schemas define the expected structure of consumption-ready data in the gold layer.
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


# Daily sales by category schema
daily_sales_by_category_schema = StructType(
    [
        StructField("transaction_date", DateType(), False),
        StructField("category", StringType(), False),
        StructField("transaction_count", IntegerType(), False),
        StructField("total_sales", DoubleType(), False),
        StructField("avg_sale_amount", DoubleType(), False),
        StructField("total_quantity", IntegerType(), False),
        # Metadata columns
        StructField("gold_insert_timestamp", TimestampType(), False),
    ]
)

# Customer purchase summary schema
customer_purchase_summary_schema = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("state", StringType(), True),
        StructField("total_transactions", IntegerType(), False),
        StructField("total_spend", DoubleType(), False),
        StructField("last_purchase_date", DateType(), True),
        # Metadata columns
        StructField("gold_insert_timestamp", TimestampType(), False),
    ]
)

# Product performance schema
product_performance_schema = StructType(
    [
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), False),
        StructField("category", StringType(), True),
        StructField("total_sales", IntegerType(), False),
        StructField("total_quantity", IntegerType(), False),
        StructField("total_revenue", DoubleType(), False),
        StructField("total_profit", DoubleType(), False),
        StructField("avg_unit_price", DoubleType(), False),
        # Metadata columns
        StructField("gold_insert_timestamp", TimestampType(), False),
    ]
)

# Data dictionary schema for documentation
data_dictionary_schema = StructType(
    [
        StructField("table_name", StringType(), False),
        StructField("column_name", StringType(), False),
        StructField("data_type", StringType(), False),
        StructField("description", StringType(), True),
        StructField("model_id", StringType(), True),
        StructField("updated_timestamp", TimestampType(), False),
    ]
)

# Schema registry for easy lookup
gold_schema_registry = {
    "gold.daily_sales_by_category": daily_sales_by_category_schema,
    "gold.customer_purchase_summary": customer_purchase_summary_schema,
    "gold.product_performance": product_performance_schema,
    "metadata.data_dictionary": data_dictionary_schema,
}

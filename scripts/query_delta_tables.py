#!/usr/bin/env python
import os
import findspark

findspark.init()

from pyspark.sql import SparkSession

# Create SparkSession
spark = (
    SparkSession.builder.appName("Delta Table Viewer")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.sql.warehouse.dir", "/app/demo_data/spark-warehouse")
    .getOrCreate()
)

# Print all databases
print("\n=== DATABASES ===")
spark.sql("SHOW DATABASES").show()

# Path to the Delta tables in the warehouse
warehouse_path = "/app/demo_data/spark-warehouse"


# Function to load and display Delta table
def load_delta_table(path, table_name):
    if os.path.exists(path):
        print(f"\n--- {table_name} ---")
        try:
            df = spark.read.format("delta").load(path)
            print(f"Row count: {df.count()}")
            df.show(10, truncate=False)
        except Exception as e:
            print(f"Error: {str(e)}")


# BRONZE LAYER TABLES
print("\n=== BRONZE LAYER TABLES ===")
bronze_tables = {
    "sales_transactions": "bronze.db/sales_transactions",
    "products": "bronze.db/products",
    "customers": "bronze.db/customers",
}

for table_name, table_path in bronze_tables.items():
    full_path = os.path.join(warehouse_path, table_path)
    load_delta_table(full_path, table_name)

# SILVER LAYER TABLES
print("\n=== SILVER LAYER TABLES ===")
silver_tables = {
    "products": "silver.db/products",
    "customers": "silver.db/customers",
    "sales_clean": "silver.db/sales_clean",
}

for table_name, table_path in silver_tables.items():
    full_path = os.path.join(warehouse_path, table_path)
    load_delta_table(full_path, table_name)

# GOLD LAYER TABLES
print("\n=== GOLD LAYER TABLES ===")
gold_tables = {
    "customer_purchase_summary": "gold.db/customer_purchase_summary",
    "daily_sales_by_category": "gold.db/daily_sales_by_category",
    "product_performance": "gold.db/product_performance",
}

for table_name, table_path in gold_tables.items():
    full_path = os.path.join(warehouse_path, table_path)
    load_delta_table(full_path, table_name)

spark.stop()

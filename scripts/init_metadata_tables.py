#!/usr/bin/env python
"""
Script to initialize metadata tables for the ETL framework.
Creates necessary databases, tables, and control metadata.
"""

import os
import sys
import findspark

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    IntegerType,
    DateType,
    BooleanType,
)

# Add the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.utils.metadata_manager import initialize_metadata_tables
from src.utils.db_utils import create_database_if_not_exists, create_table_if_not_exists
from src.schema.silver_schemas import data_quality_metrics_schema
from src.schema.gold_schemas import data_dictionary_schema


def main():
    """
    Main function to initialize metadata tables.
    """
    # Set warehouse directory to persist metadata between sessions
    warehouse_dir = "/app/demo_data/spark-warehouse"

    # Create SparkSession
    spark = (
        SparkSession.builder.appName("ETL Metadata Initialization")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .getOrCreate()
    )

    print("Creating databases...")
    # Create necessary databases
    create_database_if_not_exists(spark, "metadata")
    create_database_if_not_exists(spark, "bronze")
    create_database_if_not_exists(spark, "silver")
    create_database_if_not_exists(spark, "gold")

    print("Initializing metadata tables...")
    # Initialize ETL control table
    initialize_metadata_tables(spark)

    # Create data quality metrics table
    create_data_quality_metrics_table(spark)

    # Create data dictionary table
    create_data_dictionary_table(spark)

    print("Metadata tables initialized successfully.")
    spark.stop()


def create_data_quality_metrics_table(spark):
    """
    Create the data quality metrics table.

    Args:
        spark: SparkSession to use for data processing
    """
    # Drop table if exists
    spark.sql("DROP TABLE IF EXISTS metadata.data_quality_metrics")

    # Using the schema defined in silver_schemas.py
    from src.schema.silver_schemas import data_quality_metrics_schema

    # Create empty DataFrame with schema
    empty_df = spark.createDataFrame([], data_quality_metrics_schema)

    # Write as Delta table
    empty_df.write.format("delta").mode("overwrite").saveAsTable(
        "metadata.data_quality_metrics"
    )


def create_data_dictionary_table(spark):
    """
    Create the data dictionary table.

    Args:
        spark: SparkSession to use for data processing
    """
    # Drop table if exists
    spark.sql("DROP TABLE IF EXISTS metadata.data_dictionary")

    # Using the schema defined in gold_schemas.py
    from src.schema.gold_schemas import data_dictionary_schema

    # Create empty DataFrame with schema
    empty_df = spark.createDataFrame([], data_dictionary_schema)

    # Write as Delta table
    empty_df.write.format("delta").mode("overwrite").saveAsTable(
        "metadata.data_dictionary"
    )


if __name__ == "__main__":
    main()

#!/usr/bin/env python
"""
ETL Pipeline Runner

This script runs the complete ETL pipeline across bronze, silver, and gold layers.
It processes data according to the configuration files and provides detailed reporting.
"""

import os
import sys
import argparse
import datetime
import time
import findspark

findspark.init()

from pyspark.sql import SparkSession
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("etl_pipeline")

# Add the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.modules.bronze_layer import BronzeLayer
from src.modules.silver_layer import SilverLayer
from src.modules.gold_layer import GoldLayer
from src.utils.metadata_manager import initialize_metadata_tables, list_tables_by_layer
from src.utils.db_utils import create_database_if_not_exists


def main():
    """Main function to run the ETL pipeline."""
    parser = argparse.ArgumentParser(description="Run the ETL pipeline")

    # Processing date
    parser.add_argument(
        "--date",
        type=str,
        default=datetime.datetime.now().strftime("%Y-%m-%d"),
        help="Processing date (default: current date)",
    )

    # Layer selection options
    parser.add_argument(
        "--layers",
        type=str,
        default="bronze,silver,gold",
        help="Comma-separated list of layers to process (bronze,silver,gold)",
    )

    # Configuration
    parser.add_argument(
        "--config-dir",
        type=str,
        default="src/config",
        help="Directory containing configuration files",
    )

    # Parse arguments
    args = parser.parse_args()

    # Parse layers to process
    layers_to_process = [layer.strip().lower() for layer in args.layers.split(",")]

    # Configuration paths
    bronze_config = os.path.join(args.config_dir, "bronze_config.yaml")
    silver_config = os.path.join(args.config_dir, "silver_config.yaml")
    gold_config = os.path.join(args.config_dir, "gold_config.yaml")

    logger.info("===============================================================")
    logger.info("Starting ETL Pipeline")
    logger.info("===============================================================")
    logger.info(f"Processing Date: {args.date}")
    logger.info(f"Layers to Process: {', '.join(layers_to_process)}")

    # Create SparkSession
    logger.info("Initializing Spark Session")
    # Set warehouse directory to persist metadata between sessions
    warehouse_dir = "/app/demo_data/spark-warehouse"

    spark = (
        SparkSession.builder.appName("ETL Pipeline")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .getOrCreate()
    )

    # Ensure databases exist
    logger.info("Creating databases if they don't exist...")
    create_database_if_not_exists(spark, "metadata")
    create_database_if_not_exists(spark, "bronze")
    create_database_if_not_exists(spark, "silver")
    create_database_if_not_exists(spark, "gold")
    logger.info("Databases created successfully")

    # Initialize metadata tables
    logger.info("Initializing metadata tables...")
    initialize_metadata_tables(spark)
    logger.info("Metadata tables initialized successfully")

    # Set processing date
    processing_date = args.date
    logger.info(f"Running ETL pipeline for date: {processing_date}")

    # Track execution time and results
    start_time = time.time()
    all_results = []

    # Run Bronze layer
    if "bronze" in layers_to_process:
        logger.info("\n===============================================================")
        logger.info("BRONZE LAYER: STARTING DATA INGESTION")
        logger.info("===============================================================")
        layer_start = time.time()

        logger.info("Initializing Bronze Layer...")
        bronze_layer = BronzeLayer(spark, bronze_config)

        logger.info("Starting Bronze Layer data ingestion...")
        bronze_results = bronze_layer.ingest_all_sources(run_date=processing_date)

        duration = time.time() - layer_start
        all_results.append(("Bronze", bronze_results, duration))

        success_count = sum(
            1 for result in bronze_results if result.get("status") == "success"
        )
        error_count = sum(
            1 for result in bronze_results if result.get("status") == "error"
        )

        logger.info("--------------------------------------------------------------")
        logger.info(f"Bronze Layer Summary:")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Sources Processed: {len(bronze_results)}")
        logger.info(f"Successful: {success_count}")
        logger.info(f"Failed: {error_count}")
        if error_count > 0:
            logger.error("Errors in Bronze Layer:")
            for result in bronze_results:
                if result.get("status") == "error":
                    logger.error(
                        f"  Source: {result['source_id']}, Error: {result.get('error', 'Unknown error')}"
                    )
        logger.info("BRONZE LAYER: COMPLETED")

    # Run Silver layer
    if "silver" in layers_to_process:
        logger.info("\n===============================================================")
        logger.info("SILVER LAYER: STARTING DATA TRANSFORMATIONS")
        logger.info("===============================================================")
        layer_start = time.time()

        logger.info("Initializing Silver Layer...")
        silver_layer = SilverLayer(spark, silver_config)

        logger.info("Starting Silver Layer transformations...")
        silver_results = silver_layer.process_all_transformations(
            run_date=processing_date
        )

        duration = time.time() - layer_start
        all_results.append(("Silver", silver_results, duration))

        success_count = sum(
            1 for result in silver_results if result.get("status") == "success"
        )
        error_count = sum(
            1 for result in silver_results if result.get("status") == "error"
        )

        logger.info("--------------------------------------------------------------")
        logger.info(f"Silver Layer Summary:")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Transformations Processed: {len(silver_results)}")
        logger.info(f"Successful: {success_count}")
        logger.info(f"Failed: {error_count}")
        if error_count > 0:
            logger.error("Errors in Silver Layer:")
            for result in silver_results:
                if result.get("status") == "error":
                    logger.error(
                        f"  Transform: {result['transform_id']}, Error: {result.get('error', 'Unknown error')}"
                    )
        logger.info("SILVER LAYER: COMPLETED")

    # Run Gold layer
    if "gold" in layers_to_process:
        logger.info("\n===============================================================")
        logger.info("GOLD LAYER: STARTING ANALYTICAL MODEL GENERATION")
        logger.info("===============================================================")
        layer_start = time.time()

        logger.info("Initializing Gold Layer...")
        gold_layer = GoldLayer(spark, gold_config)

        logger.info("Starting Gold Layer model generation...")
        gold_results = gold_layer.process_all_models(run_date=processing_date)

        duration = time.time() - layer_start
        all_results.append(("Gold", gold_results, duration))

        success_count = sum(
            1 for result in gold_results if result.get("status") == "success"
        )
        error_count = sum(
            1 for result in gold_results if result.get("status") == "error"
        )

        logger.info("--------------------------------------------------------------")
        logger.info(f"Gold Layer Summary:")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Models Processed: {len(gold_results)}")
        logger.info(f"Successful: {success_count}")
        logger.info(f"Failed: {error_count}")
        if error_count > 0:
            logger.error("Errors in Gold Layer:")
            for result in gold_results:
                if result.get("status") == "error":
                    logger.error(
                        f"  Model: {result['model_id']}, Error: {result.get('error', 'Unknown error')}"
                    )
        logger.info("GOLD LAYER: COMPLETED")

    # Print summary
    total_duration = time.time() - start_time
    logger.info("\n===============================================================")
    logger.info("ETL PIPELINE SUMMARY")
    logger.info("===============================================================")
    logger.info(f"Processing Date: {processing_date}")
    logger.info(f"Total Duration: {total_duration:.2f} seconds")

    # Print data metrics if tables were processed
    if all_results:
        logger.info("\n===============================================================")
        logger.info("DATA PROCESSED")
        logger.info("===============================================================")

        if "bronze" in layers_to_process:
            logger.info("\nBronze Tables:")
            bronze_tables = list_tables_by_layer(spark, "bronze").collect()
            for table in bronze_tables:
                if table["last_run_date"] == processing_date:
                    logger.info(
                        f"  {table['table_name']}: {table['records_processed']} records"
                    )

        if "silver" in layers_to_process:
            logger.info("\nSilver Tables:")
            silver_tables = list_tables_by_layer(spark, "silver").collect()
            for table in silver_tables:
                if table["last_run_date"] == processing_date:
                    logger.info(
                        f"  {table['table_name']}: {table['records_processed']} records"
                    )

        if "gold" in layers_to_process:
            logger.info("\nGold Tables:")
            gold_tables = list_tables_by_layer(spark, "gold").collect()
            for table in gold_tables:
                if table["last_run_date"] == processing_date:
                    logger.info(
                        f"  {table['table_name']}: {table['records_processed']} records"
                    )

    logger.info("\n===============================================================")
    logger.info("ETL PIPELINE EXECUTION COMPLETED SUCCESSFULLY")
    logger.info("===============================================================")

    spark.stop()


def print_usage_examples():
    """Print examples of how to use the script."""
    print("\nUsage Examples:")
    print("  # Run all layers")
    print("  python scripts/run_etl_pipeline.py")
    print("\n  # Run only bronze layer")
    print("  python scripts/run_etl_pipeline.py --layers bronze")
    print("\n  # Run silver and gold layers")
    print("  python scripts/run_etl_pipeline.py --layers silver,gold")
    print("\n  # Process data for a specific date")
    print("  python scripts/run_etl_pipeline.py --date 2023-05-15")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--help-examples":
        print_usage_examples()
    else:
        main()

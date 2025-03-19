#!/usr/bin/env python
"""
Script to generate sample data for testing the ETL pipeline.
"""

import os
import sys
import argparse
import findspark
findspark.init()

from pyspark.sql import SparkSession

# Add the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.utils.data_generator import DataGenerator


def main():
    """
    Main function to generate sample data.
    """
    parser = argparse.ArgumentParser(description='Generate sample data for ETL testing')
    parser.add_argument('--output-path', type=str, default='/app/demo_data/raw',
                        help='Output path for the generated data (default: /app/demo_data/raw)')
    args = parser.parse_args()
    
    # Create SparkSession
    # Set warehouse directory to persist metadata between sessions
    warehouse_dir = "/app/demo_data/spark-warehouse"
    
    spark = SparkSession.builder \
        .appName("Sample Data Generator") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.warehouse.dir", warehouse_dir) \
        .getOrCreate()
    
    # Ensure output path exists
    output_path = args.output_path
    os.makedirs(output_path, exist_ok=True)
    
    print(f"Generating sample data to {output_path}...")
    
    # Initialize data generator
    generator = DataGenerator(spark)
    
    # Generate and save data
    generator.generate_and_save_data(output_path=output_path)
    
    print("Sample data generation complete.")
    print("The following files were created:")
    print(f"- Customer data (CSV): {output_path}/customers/")
    print(f"- Product data (JSON): {output_path}/products/")
    print(f"- Transaction data (Parquet): {output_path}/transactions/")
    print("\nYou can now run the ETL pipeline to process this data.")
    
    spark.stop()


if __name__ == "__main__":
    main() 
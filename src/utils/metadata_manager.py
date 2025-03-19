"""
Metadata Manager Module for metadata-driven ETL framework.
Handles control tables and ETL metadata operations.
"""

import os
import json
import datetime
from typing import Dict, Any, List, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType


def get_control_table_name() -> str:
    """
    Get the name of the control table.
    
    Returns:
        Name of the control table
    """
    return "metadata.etl_control_table"


def initialize_metadata_tables(spark: SparkSession) -> None:
    """
    Initialize metadata tables for the ETL framework.
    Creates the control table if it doesn't exist.
    
    Args:
        spark: SparkSession to use for data processing
    """
    control_table_name = get_control_table_name()
    
    try:
        # Try to refresh tables first to make sure they can be accessed
        spark.sql("REFRESH TABLE metadata.etl_control_table")
        spark.sql("REFRESH TABLE metadata.etl_audit_log")
    except:
        pass
    
    # Drop tables if they exist
    spark.sql(f"DROP TABLE IF EXISTS {control_table_name}")
    spark.sql("DROP TABLE IF EXISTS metadata.etl_audit_log")
    
    # Create the metadata database
    spark.sql("CREATE DATABASE IF NOT EXISTS metadata")
    
    # Create control table using direct Delta table creation
    empty_control_df = spark.createDataFrame(
        [], 
        "table_name STRING, layer STRING, last_run_date STRING, records_processed INT, status STRING, config_snapshot STRING, updated_timestamp TIMESTAMP"
    )
    
    # Write the empty dataframes as Delta tables with schema overwrite
    empty_control_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(control_table_name)
    
    # Create audit log table
    audit_log_schema = StructType([
        StructField("log_id", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("layer", StringType(), False),
        StructField("operation", StringType(), False),
        StructField("component", StringType(), False),
        StructField("source_id", StringType(), True),
        StructField("target_table", StringType(), True),
        StructField("status", StringType(), False),
        StructField("rows_processed", IntegerType(), True),
        StructField("error_message", StringType(), True),
        StructField("execution_time_seconds", IntegerType(), True),
        StructField("run_id", StringType(), True),
        StructField("user", StringType(), True)
    ])
    
    empty_audit_df = spark.createDataFrame([], audit_log_schema)
    
    empty_audit_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("metadata.etl_audit_log")


def update_control_table(
    spark: SparkSession,
    layer: str,
    table_name: str,
    last_run_date: str,
    records_processed: int,
    status: str = "SUCCESS",
    config_snapshot: Optional[Dict[str, Any]] = None
) -> None:
    """
    Update the ETL control table with information about the last run.
    
    Args:
        spark: SparkSession to use for data processing
        layer: Layer name (bronze, silver, gold)
        table_name: Table name
        last_run_date: Date of the last run
        records_processed: Number of records processed
        status: Status of the process (default: SUCCESS)
        config_snapshot: Optional snapshot of configuration
    """
    control_table_name = get_control_table_name()
    
    # Convert config snapshot to JSON string if provided
    config_snapshot_json = None
    if config_snapshot:
        config_snapshot_json = json.dumps(config_snapshot)
    
    # Check if the table already has an entry
    existing_entries = spark.sql(f"""
        SELECT 1 FROM {control_table_name}
        WHERE table_name = '{table_name}'
    """).count()
    
    if existing_entries > 0:
        # Update existing entry
        spark.sql(f"""
            UPDATE {control_table_name}
            SET 
                last_run_date = '{last_run_date}',
                records_processed = {records_processed},
                status = '{status}',
                config_snapshot = {f"'{config_snapshot_json}'" if config_snapshot_json else "NULL"},
                updated_timestamp = current_timestamp()
            WHERE table_name = '{table_name}'
        """)
    else:
        # Insert new entry
        spark.sql(f"""
            INSERT INTO {control_table_name} (
                table_name,
                layer,
                last_run_date,
                records_processed,
                status,
                config_snapshot,
                updated_timestamp
            ) VALUES (
                '{table_name}',
                '{layer}',
                '{last_run_date}',
                {records_processed},
                '{status}',
                {f"'{config_snapshot_json}'" if config_snapshot_json else "NULL"},
                current_timestamp()
            )
        """)


def get_last_run_date(spark: SparkSession, table_name: str) -> Optional[str]:
    """
    Get the date of the last successful run for a table.
    
    Args:
        spark: SparkSession to use for data processing
        table_name: Table name to check
        
    Returns:
        Date of the last successful run, or None if no run found
    """
    control_table_name = get_control_table_name()
    
    result = spark.sql(f"""
        SELECT last_run_date
        FROM {control_table_name}
        WHERE table_name = '{table_name}'
          AND status = 'SUCCESS'
    """).collect()
    
    if result and len(result) > 0 and result[0][0]:
        return result[0][0]
    
    return None


def get_table_metadata(
    spark: SparkSession, 
    table_name: str
) -> Optional[Dict[str, Any]]:
    """
    Get metadata about a table from the control table.
    
    Args:
        spark: SparkSession to use for data processing
        table_name: Table name to get metadata for
        
    Returns:
        Dictionary with table metadata or None if not found
    """
    control_table_name = get_control_table_name()
    
    result = spark.sql(f"""
        SELECT 
            table_name,
            layer,
            last_run_date,
            records_processed,
            status,
            config_snapshot,
            updated_timestamp
        FROM {control_table_name}
        WHERE table_name = '{table_name}'
    """).collect()
    
    if not result or len(result) == 0:
        return None
    
    row = result[0]
    
    metadata = {
        "table_name": row['table_name'],
        "layer": row['layer'],
        "last_run_date": row['last_run_date'],
        "records_processed": row['records_processed'],
        "status": row['status'],
        "updated_timestamp": row['updated_timestamp']
    }
    
    # Parse config snapshot if present
    if row['config_snapshot']:
        metadata["config_snapshot"] = json.loads(row['config_snapshot'])
    
    return metadata


def list_tables_by_layer(
    spark: SparkSession, 
    layer: str
) -> DataFrame:
    """
    List all tables in a specific layer.
    
    Args:
        spark: SparkSession to use for data processing
        layer: Layer name (bronze, silver, gold)
        
    Returns:
        DataFrame with table metadata
    """
    control_table_name = get_control_table_name()
    
    return spark.sql(f"""
        SELECT 
            table_name,
            last_run_date,
            records_processed,
            status,
            updated_timestamp
        FROM {control_table_name}
        WHERE layer = '{layer}'
        ORDER BY table_name
    """)


def register_table_schema(
    spark: SparkSession,
    table_name: str,
    schema: StructType,
    description: str = ""
) -> None:
    """
    Register a table schema in the data dictionary.
    
    Args:
        spark: SparkSession to use for data processing
        table_name: Table name to register
        schema: Table schema as StructType
        description: Optional table description
    """
    data_dictionary_table = "metadata.data_dictionary"
    
    # Ensure data dictionary table exists
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {data_dictionary_table} (
            table_name STRING NOT NULL,
            column_name STRING NOT NULL,
            data_type STRING NOT NULL,
            description STRING,
            model_id STRING,
            updated_timestamp TIMESTAMP NOT NULL
        )
        USING delta
    """)
    
    # Create records for each field in the schema
    records = []
    for field in schema.fields:
        records.append((
            table_name,
            field.name,
            str(field.dataType),
            description,
            None,
            datetime.datetime.now()
        ))
    
    # Create DataFrame from records
    columns = ["table_name", "column_name", "data_type", "description", "model_id", "updated_timestamp"]
    records_df = spark.createDataFrame(records, columns)
    
    # Register the schema
    records_df.write.format("delta").mode("append").saveAsTable(data_dictionary_table) 
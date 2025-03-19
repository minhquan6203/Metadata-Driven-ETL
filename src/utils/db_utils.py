"""
Database utility functions for the metadata-driven ETL framework.
Provides functions to interact with various data sources.
"""

import requests
import json
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame


def get_jdbc_connection(
    spark: SparkSession, 
    connection_string: str, 
    username: str, 
    password: str, 
    query: str, 
    batch_size: int = 10000
) -> DataFrame:
    """
    Get data from a JDBC source.
    
    Args:
        spark: SparkSession to use for data processing
        connection_string: JDBC connection string
        username: Database username
        password: Database password
        query: SQL query to execute
        batch_size: Number of rows to fetch in each batch
        
    Returns:
        DataFrame with data from the JDBC source
    """
    return spark.read \
        .format("jdbc") \
        .option("url", connection_string) \
        .option("user", username) \
        .option("password", password) \
        .option("query", query) \
        .option("fetchsize", str(batch_size)) \
        .load()


def get_api_data(
    spark: SparkSession, 
    api_endpoint: str, 
    auth_type: Optional[str] = None, 
    auth_token: Optional[str] = None
) -> DataFrame:
    """
    Get data from an API endpoint.
    
    Args:
        spark: SparkSession to use for data processing
        api_endpoint: API endpoint URL
        auth_type: Authentication type (e.g., bearer_token)
        auth_token: Authentication token
        
    Returns:
        DataFrame with data from the API
    """
    headers = {}
    
    if auth_type == "bearer_token" and auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"
    
    response = requests.get(api_endpoint, headers=headers)
    response.raise_for_status()
    
    data = response.json()
    
    # Convert to DataFrame
    return spark.read.json(
        spark.sparkContext.parallelize([json.dumps(data)])
    )


def execute_delta_merge(
    spark: SparkSession, 
    source_table: str, 
    target_table: str, 
    join_condition: str, 
    matched_update: Optional[str] = None, 
    not_matched_insert: Optional[str] = None
) -> None:
    """
    Execute a Delta Lake merge operation.
    
    Args:
        spark: SparkSession to use for data processing
        source_table: Source table name
        target_table: Target table name
        join_condition: Join condition for the merge
        matched_update: SQL for updating matched records (optional)
        not_matched_insert: SQL for inserting unmatched records (optional)
    """
    if not matched_update:
        matched_update = "UPDATE SET *"
    
    if not not_matched_insert:
        not_matched_insert = "INSERT *"
    
    spark.sql(f"""
        MERGE INTO {target_table} target
        USING {source_table} source
        ON {join_condition}
        WHEN MATCHED THEN
            {matched_update}
        WHEN NOT MATCHED THEN
            {not_matched_insert}
    """)


def create_database_if_not_exists(spark: SparkSession, database_name: str) -> None:
    """
    Create a database if it doesn't exist.
    
    Args:
        spark: SparkSession to use for data processing
        database_name: Database name to create
    """
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")


def create_table_if_not_exists(
    spark: SparkSession, 
    table_name: str, 
    schema_ddl: str, 
    location: Optional[str] = None,
    format: str = "delta",
    partition_by: Optional[str] = None
) -> None:
    """
    Create a table if it doesn't exist.
    
    Args:
        spark: SparkSession to use for data processing
        table_name: Table name to create
        schema_ddl: Schema DDL for the table
        location: Table location (optional)
        format: Table format (default: delta)
        partition_by: Partition columns (optional)
    """
    # First drop the table if it exists
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    
    # Parse the schema DDL into a schema definition
    schema_fields = []
    for field_def in schema_ddl.strip().split(','):
        field_parts = field_def.strip().split()
        if len(field_parts) >= 2:
            field_name = field_parts[0]
            field_type = field_parts[1]
            
            # Convert SQL type to Spark type
            if field_type.upper() == 'STRING':
                from pyspark.sql.types import StringType
                field_type = StringType()
            elif field_type.upper() == 'INT' or field_type.upper() == 'INTEGER':
                from pyspark.sql.types import IntegerType
                field_type = IntegerType()
            elif field_type.upper() == 'TIMESTAMP':
                from pyspark.sql.types import TimestampType
                field_type = TimestampType()
            elif field_type.upper() == 'BOOLEAN':
                from pyspark.sql.types import BooleanType
                field_type = BooleanType()
            elif field_type.upper() == 'DOUBLE':
                from pyspark.sql.types import DoubleType
                field_type = DoubleType()
            elif field_type.upper() == 'DATE':
                from pyspark.sql.types import DateType
                field_type = DateType()
            else:
                from pyspark.sql.types import StringType
                field_type = StringType()
            
            # Check if field is nullable
            nullable = True
            if len(field_parts) > 2 and 'NOT' in [p.upper() for p in field_parts[2:]]:
                nullable = False
            
            from pyspark.sql.types import StructField
            schema_fields.append(StructField(field_name, field_type, nullable))
    
    from pyspark.sql.types import StructType
    schema = StructType(schema_fields)
    
    # Create an empty DataFrame with the schema
    empty_df = spark.createDataFrame([], schema)
    
    # Write the empty DataFrame as a table
    writer = empty_df.write.format(format).mode("overwrite")
    
    if location:
        writer = writer.option("path", location)
    
    if partition_by:
        writer = writer.partitionBy(partition_by)
    
    writer.saveAsTable(table_name)


def truncate_table(spark: SparkSession, table_name: str) -> None:
    """
    Truncate a table.
    
    Args:
        spark: SparkSession to use for data processing
        table_name: Table name to truncate
    """
    spark.sql(f"TRUNCATE TABLE {table_name}")


def drop_table(spark: SparkSession, table_name: str, if_exists: bool = True) -> None:
    """
    Drop a table.
    
    Args:
        spark: SparkSession to use for data processing
        table_name: Table name to drop
        if_exists: Only drop if the table exists (default: True)
    """
    if_exists_clause = "IF EXISTS" if if_exists else ""
    spark.sql(f"DROP TABLE {if_exists_clause} {table_name}")


def vacuum_table(spark: SparkSession, table_name: str, retention_hours: int = 168) -> None:
    """
    Vacuum a Delta table to clean up old files.
    
    Args:
        spark: SparkSession to use for data processing
        table_name: Table name to vacuum
        retention_hours: Retention period in hours (default: 168, which is 7 days)
    """
    # Disable retention check (optional)
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    
    # Execute vacuum
    spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS") 
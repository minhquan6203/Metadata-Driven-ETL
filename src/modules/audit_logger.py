"""
Audit Logger Module for metadata-driven ETL framework.
Handles logging of ETL operations and metrics for auditing and monitoring.
"""

import os
import sys
import datetime
import uuid
import getpass
from typing import List, Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    IntegerType,
    LongType,
    DateType,
)


class AuditLogger:
    """
    Handles logging of ETL operations for auditing and monitoring.
    Logs are stored in Delta tables for easy querying and reporting.
    """

    def __init__(self, spark: SparkSession, component_name: str):
        """
        Initialize AuditLogger with SparkSession and component name.

        Args:
            spark: SparkSession to use for data processing
            component_name: Name of the component using the logger
        """
        self.spark = spark
        self.component_name = component_name
        self.audit_table = "metadata.etl_audit_log"
        self.create_audit_table_if_not_exists()

    def create_audit_table_if_not_exists(self) -> None:
        """Create the audit log table if it doesn't exist."""
        # Use the metadata manager to initialize tables - they already contain the audit table
        from src.utils.metadata_manager import initialize_metadata_tables

        initialize_metadata_tables(self.spark)

    def _get_log_schema(self) -> StructType:
        """
        Define the schema for audit log entries.

        Returns:
            StructType: Schema for audit logs
        """
        return StructType(
            [
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
                StructField("user", StringType(), True),
            ]
        )

    def log_start(
        self,
        layer: str,
        operation: str,
        source_id: str = None,
        target_table: str = None,
        run_id: str = None,
    ) -> str:
        """
        Log the start of an ETL operation.

        Args:
            layer: Data layer (bronze, silver, gold)
            operation: Operation type (ingest, transform, model)
            source_id: Source identifier
            target_table: Target table name
            run_id: Run identifier for grouping related operations

        Returns:
            log_id: Unique identifier for this log entry
        """
        log_id = str(uuid.uuid4())
        if not run_id:
            run_id = str(uuid.uuid4())

        log_data = [
            {
                "log_id": log_id,
                "timestamp": datetime.datetime.now(),
                "layer": layer,
                "operation": operation,
                "component": self.component_name,
                "source_id": source_id,
                "target_table": target_table,
                "status": "STARTED",
                "rows_processed": None,
                "error_message": None,
                "execution_time_seconds": None,
                "run_id": run_id,
                "user": self._get_current_user(),
            }
        ]

        # Write to audit log table
        log_df = self.spark.createDataFrame(log_data, schema=self._get_log_schema())
        log_df.write.format("delta").mode("append").saveAsTable(self.audit_table)

        return log_id

    def log_success(
        self,
        layer: str,
        operation: str,
        source_id: str = None,
        target_table: str = None,
        rows_processed: int = None,
        execution_time_seconds: int = None,
        run_id: str = None,
    ) -> None:
        """
        Log the successful completion of an ETL operation.

        Args:
            layer: Data layer (bronze, silver, gold)
            operation: Operation type (ingest, transform, model)
            source_id: Source identifier
            target_table: Target table name
            rows_processed: Number of rows processed
            execution_time_seconds: Execution time in seconds
            run_id: Run identifier for grouping related operations
        """
        log_id = str(uuid.uuid4())
        if not run_id:
            run_id = str(uuid.uuid4())

        log_data = [
            {
                "log_id": log_id,
                "timestamp": datetime.datetime.now(),
                "layer": layer,
                "operation": operation,
                "component": self.component_name,
                "source_id": source_id,
                "target_table": target_table,
                "status": "SUCCESS",
                "rows_processed": rows_processed,
                "error_message": None,
                "execution_time_seconds": execution_time_seconds,
                "run_id": run_id,
                "user": self._get_current_user(),
            }
        ]

        log_df = self.spark.createDataFrame(log_data, schema=self._get_log_schema())
        log_df.write.format("delta").mode("append").saveAsTable(self.audit_table)

    def log_error(
        self,
        layer: str,
        operation: str,
        source_id: str = None,
        target_table: str = None,
        error_message: str = None,
        execution_time_seconds: int = None,
        run_id: str = None,
    ) -> None:
        """
        Log an error in an ETL operation.

        Args:
            layer: Data layer (bronze, silver, gold)
            operation: Operation type (ingest, transform, model)
            source_id: Source identifier
            target_table: Target table name
            error_message: Error message
            execution_time_seconds: Execution time in seconds
            run_id: Run identifier for grouping related operations
        """
        log_id = str(uuid.uuid4())
        if not run_id:
            run_id = str(uuid.uuid4())

        log_data = [
            {
                "log_id": log_id,
                "timestamp": datetime.datetime.now(),
                "layer": layer,
                "operation": operation,
                "component": self.component_name,
                "source_id": source_id,
                "target_table": target_table,
                "status": "ERROR",
                "rows_processed": None,
                "error_message": error_message,
                "execution_time_seconds": execution_time_seconds,
                "run_id": run_id,
                "user": self._get_current_user(),
            }
        ]

        log_df = self.spark.createDataFrame(log_data, schema=self._get_log_schema())
        log_df.write.format("delta").mode("append").saveAsTable(self.audit_table)

    def _get_current_user(self) -> str:
        """
        Get the current user running the process.

        Returns:
            Current username
        """
        try:
            return self.spark.sparkContext.sparkUser()
        except:
            return "unknown"

    def get_logs_for_run(self, run_id: str) -> Any:
        """
        Get all logs for a specific run.

        Args:
            run_id: Run identifier

        Returns:
            DataFrame with logs for the run
        """
        return self.spark.sql(
            f"""
            SELECT * FROM {self.audit_table}
            WHERE run_id = '{run_id}'
            ORDER BY timestamp
        """
        )

    def get_latest_logs(self, limit: int = 100) -> Any:
        """
        Get the latest logs.

        Args:
            limit: Maximum number of logs to return

        Returns:
            DataFrame with the latest logs
        """
        return self.spark.sql(
            f"""
            SELECT * FROM {self.audit_table}
            ORDER BY timestamp DESC
            LIMIT {limit}
        """
        )

    def get_logs_for_table(self, table_name: str, limit: int = 100) -> Any:
        """
        Get logs for a specific table.

        Args:
            table_name: Table name
            limit: Maximum number of logs to return

        Returns:
            DataFrame with logs for the table
        """
        return self.spark.sql(
            f"""
            SELECT * FROM {self.audit_table}
            WHERE target_table = '{table_name}'
            ORDER BY timestamp DESC
            LIMIT {limit}
        """
        )

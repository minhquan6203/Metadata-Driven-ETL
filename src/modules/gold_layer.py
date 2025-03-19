"""
Gold Layer Module for metadata-driven ETL framework.
Handles creation of consumption-ready data models for analytics and reporting.
"""

import os
import yaml
import datetime
from typing import Dict, Any, List
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

from src.utils.metadata_manager import update_control_table
from src.modules.audit_logger import AuditLogger


class GoldLayer:
    """
    Handles creation of consumption-ready data models in the gold layer.
    Uses configuration-driven approach for data modeling and aggregations.
    """
    
    def __init__(self, spark: SparkSession, config_path: str):
        """
        Initialize GoldLayer with SparkSession and configuration path.
        
        Args:
            spark: SparkSession to use for data processing
            config_path: Path to gold layer configuration file
        """
        self.spark = spark
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)
        
        self.default_config = self.config.get('default', {})
        self.logger = AuditLogger(spark, "gold_layer")
        
        # Apply default Spark configurations
        for key, value in self.default_config.get('spark_conf', {}).items():
            spark.conf.set(key, value)
    
    def process_all_models(self, run_date: str = None) -> List[Dict[str, Any]]:
        """
        Process all enabled data models defined in configuration.
        
        Args:
            run_date: Optional run date for processing, defaults to current date
            
        Returns:
            List of processing results for each model
        """
        if not run_date:
            run_date = datetime.datetime.now().strftime("%Y-%m-%d")
        
        results = []
        for model in self.config.get('models', []):
            if model.get('enabled', True):
                try:
                    self.logger.log_start(
                        layer="gold",
                        operation="model",
                        source_id=model['model_id'],
                        target_table=model['target_table']
                    )
                    
                    result = self._process_model(model, run_date)
                    results.append(result)
                    
                    self.logger.log_success(
                        layer="gold",
                        operation="model",
                        source_id=model['model_id'],
                        target_table=model['target_table'],
                        rows_processed=result['rows_processed']
                    )
                    
                except Exception as e:
                    self.logger.log_error(
                        layer="gold",
                        operation="model",
                        source_id=model['model_id'],
                        target_table=model['target_table'],
                        error_message=str(e)
                    )
                    results.append({
                        'model_id': model['model_id'],
                        'status': 'error',
                        'error': str(e)
                    })
        
        return results
    
    def _process_model(self, model: Dict[str, Any], run_date: str) -> Dict[str, Any]:
        """
        Process a single data model based on its configuration.
        
        Args:
            model: Model configuration dictionary
            run_date: Processing date
            
        Returns:
            Dictionary with processing results
        """
        model_id = model['model_id']
        target_table = model['target_table']
        model_type = model.get('model_type', 'sql')
        refresh_type = model.get('refresh_type', 'full')
        
        # Replace processing date in SQL query
        if model_type == 'sql':
            sql_query = model['sql_query'].replace('${PROCESSING_DATE}', run_date)
            df = self.spark.sql(sql_query)
        else:
            raise ValueError(f"Unsupported model type: {model_type}")
        
        # Add metadata columns
        df = df.withColumn("gold_insert_timestamp", F.current_timestamp())
        
        # Determine write mode based on refresh type
        write_mode = "overwrite"
        if refresh_type == 'incremental':
            write_mode = "merge"
            
            # Check if target table exists
            if not self._table_exists(target_table):
                write_mode = "overwrite"
        
        # Write to target table with specified options
        rows_processed = df.count()
        
        write_options = {}
        if 'partition_by' in model:
            write_options['partitionBy'] = model['partition_by']
        
        # Handle merge operation for incremental loads
        if write_mode == "merge" and 'primary_keys' in model:
            self._merge_data(df, target_table, model['primary_keys'])
        else:
            df.write \
                .format("delta") \
                .options(**write_options) \
                .mode("overwrite") \
                .saveAsTable(target_table)
        
        # Update data catalog with metadata
        self._update_documentation(model, target_table)
        
        # Update control table with run information
        update_control_table(
            self.spark,
            layer="gold",
            table_name=target_table,
            last_run_date=run_date,
            records_processed=rows_processed
        )
        
        return {
            'model_id': model_id,
            'status': 'success',
            'target_table': target_table,
            'rows_processed': rows_processed,
            'run_date': run_date
        }
    
    def _table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the catalog.
        
        Args:
            table_name: Table name to check
            
        Returns:
            True if table exists, False otherwise
        """
        tables = self.spark.catalog.listTables()
        for table in tables:
            full_name = f"{table.database}.{table.name}" if table.database else table.name
            if full_name.lower() == table_name.lower():
                return True
        return False
    
    def _merge_data(self, source_df: DataFrame, target_table: str, merge_keys: List[str]) -> None:
        """
        Perform a merge operation for incremental load.
        
        Args:
            source_df: Source DataFrame with new data
            target_table: Target table name
            merge_keys: List of columns to use as merge keys
        """
        merge_condition = " AND ".join([f"source.{key} = target.{key}" for key in merge_keys])
        
        # Register temporary view for the source
        source_df.createOrReplaceTempView("source_data")
        
        # Execute merge using Delta Lake
        self.spark.sql(f"""
            MERGE INTO {target_table} target
            USING source_data source
            ON {merge_condition}
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *
        """)
    
    def _update_documentation(self, model: Dict[str, Any], target_table: str) -> None:
        """
        Update data dictionary with metadata about the table.
        
        Args:
            model: Model configuration
            target_table: Target table name
        """
        # Disabled documentation table updates to avoid Delta table issues
        return
        
        documentation_table = self.default_config.get('documentation_table')
        if not documentation_table:
            return
        
        # Get schema information
        schema_df = self.spark.sql(f"DESCRIBE TABLE {target_table}")
        
        # Create documentation records
        docs_records = []
        for row in schema_df.collect():
            if row["col_name"] != "# col_name":
                docs_records.append({
                    "table_name": target_table,
                    "column_name": row["col_name"],
                    "data_type": row["data_type"],
                    "description": model.get('description', ''),
                    "model_id": model['model_id'],
                    "updated_timestamp": datetime.datetime.now().isoformat()
                })
        
        if docs_records:
            docs_df = self.spark.createDataFrame(docs_records)
            
            # Write to documentation table
            docs_df.write \
                .format("delta") \
                .mode("append") \
                .saveAsTable(documentation_table) 
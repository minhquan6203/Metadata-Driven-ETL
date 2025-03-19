"""
Bronze Layer Module for metadata-driven ETL framework.
Handles raw data ingestion from source systems based on configuration.
"""

import os
import yaml
import datetime
from typing import Dict, Any, List
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

from src.utils.db_utils import get_jdbc_connection, get_api_data
from src.utils.metadata_manager import update_control_table, get_last_run_date
from src.modules.audit_logger import AuditLogger


class BronzeLayer:
    """
    Handles raw data ingestion from source systems to bronze layer.
    Uses configuration-driven approach to load data from various sources.
    """
    
    def __init__(self, spark: SparkSession, config_path: str):
        """
        Initialize BronzeLayer with SparkSession and configuration path.
        
        Args:
            spark: SparkSession to use for data processing
            config_path: Path to bronze layer configuration file
        """
        self.spark = spark
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)
        
        self.default_config = self.config.get('default', {})
        self.logger = AuditLogger(spark, "bronze_layer")
        
        # Apply default Spark configurations
        for key, value in self.default_config.get('spark_conf', {}).items():
            spark.conf.set(key, value)
    
    def ingest_all_sources(self, run_date: str = None) -> List[Dict[str, Any]]:
        """
        Ingest data from all enabled sources defined in configuration.
        
        Args:
            run_date: Optional run date for processing, defaults to current date
            
        Returns:
            List of processing results for each source
        """
        if not run_date:
            run_date = datetime.datetime.now().strftime("%Y-%m-%d")
        
        results = []
        for source in self.config.get('sources', []):
            if source.get('enabled', True):
                try:
                    self.logger.log_start(
                        layer="bronze",
                        operation="ingest",
                        source_id=source['source_id'],
                        target_table=source['target_table']
                    )
                    
                    result = self._ingest_source(source, run_date)
                    results.append(result)
                    
                    self.logger.log_success(
                        layer="bronze",
                        operation="ingest",
                        source_id=source['source_id'],
                        target_table=source['target_table'],
                        rows_processed=result['rows_processed']
                    )
                    
                except Exception as e:
                    self.logger.log_error(
                        layer="bronze",
                        operation="ingest",
                        source_id=source['source_id'],
                        target_table=source['target_table'],
                        error_message=str(e)
                    )
                    results.append({
                        'source_id': source['source_id'],
                        'status': 'error',
                        'error': str(e)
                    })
        
        return results
    
    def _ingest_source(self, source: Dict[str, Any], run_date: str) -> Dict[str, Any]:
        """
        Ingest data from a single source based on its configuration.
        
        Args:
            source: Source configuration dictionary
            run_date: Processing date
            
        Returns:
            Dictionary with processing results
        """
        source_id = source['source_id']
        source_type = source['source_type']
        target_table = source['target_table']
        
        # Get extraction date based on strategy
        extract_date = run_date
        if source.get('extract_strategy') == 'incremental':
            last_run_date = get_last_run_date(self.spark, target_table)
            if last_run_date:
                extract_date = last_run_date
        
        # Extract data based on source type
        if source_type == 'jdbc':
            df = self._extract_jdbc(source, extract_date)
        elif source_type == 'file':
            df = self._extract_file(source)
        elif source_type == 'api':
            df = self._extract_api(source)
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
        
        # Add metadata columns
        df = df.withColumn("ingestion_timestamp", F.current_timestamp())
        df = df.withColumn("source_id", F.lit(source_id))
        
        # Write to target table
        rows_processed = df.count()
        
        df.write \
            .format(source.get('format', 'delta')) \
            .mode("append") \
            .saveAsTable(target_table)
        
        # Update control table with run information
        update_control_table(
            self.spark,
            layer="bronze",
            table_name=target_table,
            last_run_date=run_date,
            records_processed=rows_processed
        )
        
        return {
            'source_id': source_id,
            'status': 'success',
            'target_table': target_table,
            'rows_processed': rows_processed,
            'run_date': run_date
        }
    
    def _extract_jdbc(self, source: Dict[str, Any], extract_date: str) -> DataFrame:
        """
        Extract data from JDBC source.
        
        Args:
            source: Source configuration
            extract_date: Date to use for extraction
            
        Returns:
            DataFrame with extracted data
        """
        connection_string = source['connection_string']
        username = os.environ.get(source['username'].replace('${', '').replace('}', ''))
        password = os.environ.get(source['password'].replace('${', '').replace('}', ''))
        
        query = source['query'].replace('${EXTRACT_DATE}', extract_date)
        
        return get_jdbc_connection(
            self.spark,
            connection_string,
            username,
            password,
            query,
            source.get('batch_size', 10000)
        )
    
    def _extract_file(self, source: Dict[str, Any]) -> DataFrame:
        """
        Extract data from file source.
        
        Args:
            source: Source configuration
            
        Returns:
            DataFrame with extracted data
        """
        source_path = source['source_path']
        file_format = source['file_format']
        options = source.get('options', {})
        
        reader = self.spark.read.format(file_format)
        for key, value in options.items():
            reader = reader.option(key, value)
        
        return reader.load(source_path)
    
    def _extract_api(self, source: Dict[str, Any]) -> DataFrame:
        """
        Extract data from API source.
        
        Args:
            source: Source configuration
            
        Returns:
            DataFrame with extracted data
        """
        api_endpoint = source['api_endpoint']
        auth_type = source.get('auth_type')
        auth_token = None
        
        if auth_type == 'bearer_token':
            token_env = source['auth_token'].replace('${', '').replace('}', '')
            auth_token = os.environ.get(token_env)
        
        return get_api_data(
            self.spark,
            api_endpoint,
            auth_type,
            auth_token
        ) 
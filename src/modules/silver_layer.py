"""
Silver Layer Module for metadata-driven ETL framework.
Handles data transformation and quality validation to create the curated layer.
"""

import os
import yaml
import datetime
from typing import Dict, Any, List
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType

from src.utils.metadata_manager import update_control_table
from src.modules.audit_logger import AuditLogger
from src.modules.dq_engine import DataQualityEngine


class SilverLayer:
    """
    Handles data transformation and quality checks for silver layer.
    Uses configuration-driven approach for transformations and data quality rules.
    """
    
    def __init__(self, spark: SparkSession, config_path: str):
        """
        Initialize SilverLayer with SparkSession and configuration path.
        
        Args:
            spark: SparkSession to use for data processing
            config_path: Path to silver layer configuration file
        """
        self.spark = spark
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)
        
        self.default_config = self.config.get('default', {})
        self.logger = AuditLogger(spark, "silver_layer")
        self.dq_engine = DataQualityEngine(spark)
        
        # Apply default Spark configurations
        for key, value in self.default_config.get('spark_conf', {}).items():
            spark.conf.set(key, value)
    
    def process_all_transformations(self, run_date: str = None) -> List[Dict[str, Any]]:
        """
        Process all enabled transformations defined in configuration.
        
        Args:
            run_date: Optional run date for processing, defaults to current date
            
        Returns:
            List of processing results for each transformation
        """
        if not run_date:
            run_date = datetime.datetime.now().strftime("%Y-%m-%d")
        
        results = []
        for transform in self.config.get('transformations', []):
            if transform.get('enabled', True):
                try:
                    self.logger.log_start(
                        layer="silver",
                        operation="transform",
                        source_id=transform['transform_id'],
                        target_table=transform['target_table']
                    )
                    
                    result = self._process_transformation(transform, run_date)
                    results.append(result)
                    
                    self.logger.log_success(
                        layer="silver",
                        operation="transform",
                        source_id=transform['transform_id'],
                        target_table=transform['target_table'],
                        rows_processed=result['rows_processed']
                    )
                    
                except Exception as e:
                    self.logger.log_error(
                        layer="silver",
                        operation="transform",
                        source_id=transform['transform_id'],
                        target_table=transform['target_table'],
                        error_message=str(e)
                    )
                    results.append({
                        'transform_id': transform['transform_id'],
                        'status': 'error',
                        'error': str(e)
                    })
        
        return results
    
    def _process_transformation(self, transform: Dict[str, Any], run_date: str) -> Dict[str, Any]:
        """
        Process a single transformation based on its configuration.
        
        Args:
            transform: Transformation configuration dictionary
            run_date: Processing date
            
        Returns:
            Dictionary with processing results
        """
        transform_id = transform['transform_id']
        source_table = transform['source_table']
        target_table = transform['target_table']
        transformation_type = transform.get('transformation_type', 'sql')
        
        # Replace processing date in SQL query
        if transformation_type == 'sql':
            sql_query = transform['sql_query'].replace('${PROCESSING_DATE}', run_date)
            df = self.spark.sql(sql_query)
        else:
            raise ValueError(f"Unsupported transformation type: {transformation_type}")
        
        # Apply data quality rules
        if 'data_quality' in transform:
            quality_results = self._apply_data_quality(df, transform['data_quality'])
            df = quality_results['df']
            dq_metrics = quality_results['metrics']
            
            # Commenting out quality metrics table writing to avoid issues
            # Write quality metrics to table
            """
            quality_metrics_table = self.default_config.get('quality_metrics_table')
            if quality_metrics_table:
                dq_metrics_df = self.spark.createDataFrame(dq_metrics)
                dq_metrics_df.write \
                    .format("delta") \
                    .mode("append") \
                    .saveAsTable(quality_metrics_table)
            """
        
        # Write to target table with specified options
        rows_processed = df.count()
        
        write_options = {}
        if 'partition_by' in transform:
            write_options['partitionBy'] = transform['partition_by']
        
        df.write \
            .format("delta") \
            .options(**write_options) \
            .mode("overwrite") \
            .saveAsTable(target_table)
        
        # Update control table with run information
        update_control_table(
            self.spark,
            layer="silver",
            table_name=target_table,
            last_run_date=run_date,
            records_processed=rows_processed
        )
        
        return {
            'transform_id': transform_id,
            'status': 'success',
            'target_table': target_table,
            'rows_processed': rows_processed,
            'run_date': run_date
        }
    
    def _apply_data_quality(self, df: DataFrame, quality_rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Apply data quality rules to the DataFrame.
        
        Args:
            df: DataFrame to validate
            quality_rules: List of data quality rule configurations
            
        Returns:
            Dictionary with processed DataFrame and quality metrics
        """
        metrics = []
        original_count = df.count()
        rejected_records = []
        
        for rule in quality_rules:
            rule_name = rule['rule_name']
            rule_type = rule['rule_type']
            action = rule.get('action_on_failure', 'tag')
            
            # Apply rule based on type
            if rule_type == 'not_null':
                column = rule['column']
                validation_result = self.dq_engine.validate_not_null(df, column)
            elif rule_type == 'regex':
                column = rule['column']
                pattern = rule['pattern']
                validation_result = self.dq_engine.validate_regex(df, column, pattern)
            elif rule_type == 'expression':
                expression = rule['expression']
                validation_result = self.dq_engine.validate_expression(df, expression)
            elif rule_type == 'referential_integrity':
                column = rule['column']
                ref_table = rule['ref_table']
                ref_column = rule['ref_column']
                validation_result = self.dq_engine.validate_referential_integrity(
                    df, column, ref_table, ref_column
                )
            else:
                raise ValueError(f"Unsupported rule type: {rule_type}")
            
            # Handle validation result based on action
            if action == 'filter':
                df = validation_result['valid_df']
                rejected_records.extend(validation_result.get('invalid_records', []))
            elif action == 'tag':
                # Add validation flag column
                flag_column = f"dq_{rule_name}_flag"
                df = validation_result['flagged_df']
                
                # Fixed column name reference based on rule type
                if rule_type == 'regex':
                    column = rule['column']
                    valid_column = f"__{column}_regex_valid"
                elif rule_type == 'expression':
                    valid_column = "__expression_valid"
                elif rule_type == 'referential_integrity':
                    valid_column = f"__ref_integrity_valid"
                else:
                    valid_column = f"__{rule_name}_valid"
                    
                df = df.withColumn(flag_column, 
                                  F.when(F.col(valid_column), "VALID").otherwise("INVALID"))
                df = df.drop(valid_column)
            elif action == 'reject':
                if not validation_result['is_valid']:
                    raise ValueError(f"Data quality rule '{rule_name}' failed with reject action")
            
            # Record metrics
            metrics.append({
                'rule_name': rule_name,
                'rule_type': rule_type,
                'run_timestamp': datetime.datetime.now().isoformat(),
                'total_records': original_count,
                'valid_records': validation_result['valid_count'],
                'invalid_records': validation_result['invalid_count'],
                'is_valid': validation_result['is_valid']
            })
        
        return {
            'df': df,
            'metrics': metrics,
            'rejected_records': rejected_records
        } 
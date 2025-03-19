"""
Data Quality Engine for the metadata-driven ETL framework.
Provides functions to validate data quality based on different rule types.
"""

from typing import Dict, Any, List
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType


class DataQualityEngine:
    """
    Provides data quality validation capabilities for the ETL framework.
    """
    
    def __init__(self, spark: SparkSession):
        """
        Initialize DataQualityEngine with SparkSession.
        
        Args:
            spark: SparkSession to use for data processing
        """
        self.spark = spark
    
    def validate_not_null(self, df: DataFrame, column: str) -> Dict[str, Any]:
        """
        Validate that a column does not contain null values.
        
        Args:
            df: DataFrame to validate
            column: Column name to check for null values
            
        Returns:
            Validation result with valid and invalid dataframes and counts
        """
        # Count null values
        null_count = df.filter(F.col(column).isNull()).count()
        total_count = df.count()
        valid_count = total_count - null_count
        
        # Create validation flag
        flagged_df = df.withColumn(f"__{column}_not_null_valid", F.col(column).isNotNull())
        
        # Split into valid and invalid dataframes
        valid_df = df.filter(F.col(column).isNotNull())
        invalid_df = df.filter(F.col(column).isNull())
        
        # Extract invalid records for logging
        invalid_records = []
        if null_count > 0:
            invalid_records = invalid_df.limit(100).collect()
        
        return {
            'is_valid': null_count == 0,
            'valid_count': valid_count,
            'invalid_count': null_count,
            'valid_df': valid_df,
            'invalid_df': invalid_df,
            'flagged_df': flagged_df,
            'invalid_records': invalid_records
        }
    
    def validate_regex(self, df: DataFrame, column: str, pattern: str) -> Dict[str, Any]:
        """
        Validate that values in a column match a regex pattern.
        
        Args:
            df: DataFrame to validate
            column: Column name to check
            pattern: Regex pattern to match
            
        Returns:
            Validation result with valid and invalid dataframes and counts
        """
        # Count values not matching the pattern
        invalid_count = df.filter(~F.col(column).rlike(pattern)).count()
        total_count = df.count()
        valid_count = total_count - invalid_count
        
        # Create validation flag
        flagged_df = df.withColumn(f"__{column}_regex_valid", F.col(column).rlike(pattern))
        
        # Split into valid and invalid dataframes
        valid_df = df.filter(F.col(column).rlike(pattern))
        invalid_df = df.filter(~F.col(column).rlike(pattern))
        
        # Extract invalid records for logging
        invalid_records = []
        if invalid_count > 0:
            invalid_records = invalid_df.limit(100).collect()
        
        return {
            'is_valid': invalid_count == 0,
            'valid_count': valid_count,
            'invalid_count': invalid_count,
            'valid_df': valid_df,
            'invalid_df': invalid_df,
            'flagged_df': flagged_df,
            'invalid_records': invalid_records
        }
    
    def validate_expression(self, df: DataFrame, expression: str) -> Dict[str, Any]:
        """
        Validate data using a SQL expression.
        
        Args:
            df: DataFrame to validate
            expression: SQL expression that evaluates to a boolean
            
        Returns:
            Validation result with valid and invalid dataframes and counts
        """
        # Count values not satisfying the expression
        invalid_count = df.filter(~F.expr(expression)).count()
        total_count = df.count()
        valid_count = total_count - invalid_count
        
        # Create validation flag
        flagged_df = df.withColumn("__expression_valid", F.expr(expression))
        
        # Split into valid and invalid dataframes
        valid_df = df.filter(F.expr(expression))
        invalid_df = df.filter(~F.expr(expression))
        
        # Extract invalid records for logging
        invalid_records = []
        if invalid_count > 0:
            invalid_records = invalid_df.limit(100).collect()
        
        return {
            'is_valid': invalid_count == 0,
            'valid_count': valid_count,
            'invalid_count': invalid_count,
            'valid_df': valid_df,
            'invalid_df': invalid_df,
            'flagged_df': flagged_df,
            'invalid_records': invalid_records
        }
    
    def validate_referential_integrity(
        self, df: DataFrame, column: str, ref_table: str, ref_column: str
    ) -> Dict[str, Any]:
        """
        Validate referential integrity between dataframes.
        
        Args:
            df: DataFrame to validate
            column: Column name to check
            ref_table: Reference table name
            ref_column: Reference column name
            
        Returns:
            Validation result with valid and invalid dataframes and counts
        """
        # Load reference data
        ref_df = self.spark.table(ref_table).select(ref_column).distinct()
        
        # Create a temporary view for the reference data
        ref_df.createOrReplaceTempView("ref_data")
        
        # Register the input dataframe as a temporary view
        df.createOrReplaceTempView("source_data")
        
        # Find invalid records (not in reference data)
        invalid_df = self.spark.sql(f"""
            SELECT s.*
            FROM source_data s
            LEFT JOIN ref_data r ON s.{column} = r.{ref_column}
            WHERE r.{ref_column} IS NULL
        """)
        
        # Count invalid records
        invalid_count = invalid_df.count()
        total_count = df.count()
        valid_count = total_count - invalid_count
        
        # Find valid records
        valid_df = self.spark.sql(f"""
            SELECT s.*
            FROM source_data s
            JOIN ref_data r ON s.{column} = r.{ref_column}
        """)
        
        # Create flagged dataframe
        df_with_flag = self.spark.sql(f"""
            SELECT s.*, 
                   CASE WHEN r.{ref_column} IS NOT NULL THEN true ELSE false END AS __{column}_ref_valid
            FROM source_data s
            LEFT JOIN ref_data r ON s.{column} = r.{ref_column}
        """)
        
        # Extract invalid records for logging
        invalid_records = []
        if invalid_count > 0:
            invalid_records = invalid_df.limit(100).collect()
        
        return {
            'is_valid': invalid_count == 0,
            'valid_count': valid_count,
            'invalid_count': invalid_count,
            'valid_df': valid_df,
            'invalid_df': invalid_df,
            'flagged_df': df_with_flag,
            'invalid_records': invalid_records
        }
    
    def validate_uniqueness(self, df: DataFrame, columns: List[str]) -> Dict[str, Any]:
        """
        Validate that values in specified columns are unique.
        
        Args:
            df: DataFrame to validate
            columns: List of column names that should be unique together
            
        Returns:
            Validation result with valid and invalid dataframes and counts
        """
        # Find duplicate records
        window_spec = F.Window.partitionBy(*columns).orderBy(F.lit(1))
        df_with_dups = df.withColumn("__row_number", F.row_number().over(window_spec))
        
        # Count invalid records (duplicates)
        invalid_df = df_with_dups.filter(F.col("__row_number") > 1).drop("__row_number")
        invalid_count = invalid_df.count()
        total_count = df.count()
        
        # Valid records are the ones that appear only once
        valid_df = df_with_dups.filter(F.col("__row_number") == 1).drop("__row_number")
        valid_count = valid_df.count()
        
        # Create flagged dataframe
        flagged_df = df.alias("a").join(
            df.groupBy(*columns).count().withColumnRenamed("count", "__count").alias("b"),
            on=[F.col(f"a.{col}") == F.col(f"b.{col}") for col in columns],
            how="left"
        ).select(
            *[F.col(f"a.{col}") for col in df.columns],
            F.col("__count") == 1
        ).withColumnRenamed("((`__count` = 1))", "__uniqueness_valid")
        
        # Extract invalid records for logging
        invalid_records = []
        if invalid_count > 0:
            invalid_records = invalid_df.limit(100).collect()
        
        return {
            'is_valid': invalid_count == 0,
            'valid_count': valid_count,
            'invalid_count': invalid_count,
            'valid_df': valid_df,
            'invalid_df': invalid_df,
            'flagged_df': flagged_df,
            'invalid_records': invalid_records
        } 
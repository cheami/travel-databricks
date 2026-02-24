from typing import Any
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

def extract_csv(path: str) -> Any:
    return (spark.read
            .format("csv")
            .option("header", True)
            .option("inferSchema", True)
            .option("quote", '"')  # Handles text qualifiers
            .load(path))

def extract_json(path: str) -> Any:
    return (spark.read
            .format("json")
            .option("multiLine", True)
            .load(path))
    
def extract_csv_stream(path: str) -> Any:
    """
    Reads CSV files incrementally using Databricks Auto Loader.
    """
    return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", True)
            # Auto Loader uses inferColumnTypes instead of inferSchema
            .option("cloudFiles.inferColumnTypes", True) 
            .option("quote", '"')
            .load(path))

def extract_json_stream(path: str) -> Any:
    """
    Reads JSON files incrementally using Databricks Auto Loader.
    """
    return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("multiLine", True)
            .option("cloudFiles.inferColumnTypes", True)
            .load(path))

def transform_columns(df: Any) -> Any:
    return df.toDF(*[col.replace(" ", "_") for col in df.columns])

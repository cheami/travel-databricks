from typing import Any
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def extract_csv(path):
    return (spark.read
            .format("csv")
            .option("header", True)
            .option("inferSchema", True)
            .option("quote", '"')  # Handles text qualifiers
            .load(path))

def extract_json(path):
    return (spark.read
            .format("json")
            .option("multiLine", True)
            .load(path))
    
def extract_csv_stream(path):
    """
    Reads CSV files incrementally using Databricks Auto Loader.
    """
    return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", True)
            .option("cloudFiles.inferColumnTypes", True) 
            .option("quote", '"')
            .load(path))

def extract_json_stream(path):
    """
    Reads JSON files incrementally using Databricks Auto Loader.
    """
    return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("multiLine", True)
            .option("cloudFiles.inferColumnTypes", True)
            .load(path))

def transform_columns(df):
    return df.toDF(*[col.replace(" ", "_") for col in df.columns])

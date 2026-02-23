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

def transform_columns(df: Any) -> Any:
    return df.toDF(*[col.replace(" ", "_") for col in df.columns])

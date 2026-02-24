from pyspark import pipelines as dp
from pyspark.sql.functions import struct
from functions.etl_functions import extract_json_stream, transform_columns

@dp.table(
    name="bronze.google_timeline",
    comment="Raw Google timeline data ingested incrementally via Auto Loader"
)
def google_timeline():
    df = extract_json_stream("/Volumes/workspace/data_landing/travel_volume/google_timeline")
    df = df.select(struct(*df.columns).alias("raw_json"))
    return df
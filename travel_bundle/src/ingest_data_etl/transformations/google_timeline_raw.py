from pyspark import pipelines as dp
from pyspark.sql.functions import struct
from functions.etl_functions import extract_json

@dp.table()
def google_timeline_raw():
    df = extract_json("/Volumes/workspace/travel/travel_data_volume/google_timeline/")
    df = df.select(struct(*df.columns).alias("raw_json"))
    return df
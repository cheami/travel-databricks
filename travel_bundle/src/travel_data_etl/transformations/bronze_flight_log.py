from pyspark import pipelines as dp
from functions.etl_functions import extract_csv_stream, transform_columns

@dp.table(
    name="bronze.flight_log",
    comment="Raw flight logs data ingested incrementally via Auto Loader"
)
def flight_log():
    df = extract_csv_stream("/Volumes/workspace/data_landing/travel_volume/flight_logs")
    df = transform_columns(df)
    return df
from pyspark import pipelines as dp
from functions.etl_functions import extract_csv_stream, transform_columns

@dp.table(
    name="bronze.manual_log",
    comment="Raw manual log data ingested incrementally via Auto Loader"
)
def manual_log():
    df = extract_csv_stream("/Volumes/workspace/data_landing/travel_volume/manual_logs")
    df = transform_columns(df)
    return df
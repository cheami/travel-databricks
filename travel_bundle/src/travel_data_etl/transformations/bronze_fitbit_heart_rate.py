from pyspark import pipelines as dp
from functions.etl_functions import extract_csv_stream, transform_columns

@dp.table(
    name="bronze.fitbit_heart_rate",
    comment="Raw Fitbit heart rate data ingested incrementally via Auto Loader"
)
def fitbit_heart_rate():
    df = extract_csv_stream("/Volumes/workspace/data_landing/travel_volume/fitbit/heart_rate")
    df = transform_columns(df)
    return df
from pyspark import pipelines as dp
from functions.etl_functions import extract_csv_stream, transform_columns

@dp.table(
    name="bronze.fitbit_sleep_score",
    comment="Raw Fitbit sleep score data ingested incrementally via Auto Loader"
)
def fitbit_sleep_score():
    df = extract_csv_stream("/Volumes/workspace/data_landing/travel_volume/fitbit/sleep_score")
    df = transform_columns(df)
    return df
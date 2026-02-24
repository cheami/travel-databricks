from pyspark import pipelines as dp
from functions.etl_functions import extract_csv_stream, transform_columns

@dp.table(
    name="bronze.fitbit_steps",
    comment="Raw Fitbit steps data ingested incrementally via Auto Loader"
)
def fitbit_steps():
    df = extract_csv_stream("/Volumes/workspace/data_landing/travel_volume/fitbit/steps")
    df = transform_columns(df)
    return df
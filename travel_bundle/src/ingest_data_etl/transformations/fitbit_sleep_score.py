from pyspark import pipelines as dp
from functions.etl_functions import extract_csv, transform_columns

@dp.table()
def fitbit_sleep_score():
    df = extract_csv("/Volumes/workspace/travel/travel_data_volume/fitbit/sleep_score/")
    df = transform_columns(df)
    return df

from pyspark import pipelines as dp
from functions.etl_functions import extract_csv, transform_columns

@dp.table()
def flight_log():
    df = extract_csv("/Volumes/workspace/travel/travel_data_volume/flight_logs/")
    df = transform_columns(df)
    return df

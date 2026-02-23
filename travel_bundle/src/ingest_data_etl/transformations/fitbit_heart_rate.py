from pyspark import pipelines as dp
from functions.etl_functions import extract_csv, transform_columns

@dp.table()
def fitbit_heart_rate():
    df = extract_csv("/Volumes/workspace/travel/travel_data_volume/fitbit/heart_rate")
    df = transform_columns(df)
    return df

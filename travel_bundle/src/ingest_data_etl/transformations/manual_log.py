from pyspark import pipelines as dp
from functions.etl_functions import extract_csv, transform_columns

@dp.table()
def manual_log():
    df = extract_csv("/Volumes/workspace/travel/travel_data_volume/manual_logs/")
    df = transform_columns(df)
    return df

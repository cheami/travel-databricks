from pyspark import pipelines as dp
from functions.etl_functions import extract_csv, transform_columns

@dp.table()
def transaction():
    df = extract_csv("/Volumes/workspace/travel/travel_data_volume/transactions/")
    df = transform_columns(df)
    return df

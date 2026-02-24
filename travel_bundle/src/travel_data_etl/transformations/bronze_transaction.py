from pyspark import pipelines as dp
from functions.etl_functions import extract_csv_stream, transform_columns

@dp.table(
    name="bronze.transaction",
    comment="Raw transaction data ingested incrementally via Auto Loader"
)
def transaction():
    df = extract_csv_stream("/Volumes/workspace/data_landing/travel_volume/transactions")
    df = transform_columns(df)
    return df
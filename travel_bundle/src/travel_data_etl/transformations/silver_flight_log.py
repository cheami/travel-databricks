from pyspark import pipelines as dp



@dp.table(
    name="silver.flight_log"
)
def flight_log():
    return spark.read.table("bronze.flight_log")
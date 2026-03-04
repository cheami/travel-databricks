from pyspark import pipelines as dp
from pyspark.sql.functions import regexp_extract, lpad, concat_ws, when, col, trim


@dp.table(
    name="silver.flight_log"
)
def flight_log():
    df = spark.read.table("bronze.flight_log")
    df = df.withColumn("From_City", regexp_extract("From", r"^([^/]+)", 1)) \
    .withColumn("From_Airport", regexp_extract("From", r"/\s*([^(]+)", 1)) \
    .withColumn("From_IATA", regexp_extract("From", r"\(([^/]+)", 1)) \
    .withColumn("From_ICAO", regexp_extract("From", r"\((?:[^/]+/)?([A-Z0-9]+)\)", 1)) \
    .withColumn("To_City", regexp_extract("To", r"^([^/]+)", 1)) \
    .withColumn("To_Airport", regexp_extract("To", r"/\s*([^(]+)", 1)) \
    .withColumn("To_IATA", regexp_extract("To", r"\(([^/]+)", 1)) \
    .withColumn("To_ICAO", regexp_extract("To", r"\((?:[^/]+/)?([A-Z0-9]+)\)", 1)) \
    .withColumn("Airline_Name", regexp_extract("Airline", r"^([^(]+)", 1)) \
    .withColumn("Airline_IATA", regexp_extract("Airline", r"\(([^/]+)", 1)) \
    .withColumn("Airline_ICAO", regexp_extract("Airline", r"/([A-Z0-9]+)\)", 1))

    df = df.withColumn(
    "Duration",
    concat_ws(
        ":",
        lpad(regexp_extract("Duration", r"(\d+):", 1), 2, "0"),
        lpad(regexp_extract("Duration", r":(\d+)", 1), 2, "0")
    ).cast("string")) \
    .withColumn(
    "Dep_Time",
    concat_ws(
        ":",
        lpad(regexp_extract("Dep_Time", r"(\d+):", 1), 2, "0"),
        lpad(regexp_extract("Dep_Time", r":(\d+)", 1), 2, "0")
    ).cast("string")) \
    .withColumn(
    "Arr_Time",
    concat_ws(
        ":",
        lpad(regexp_extract("Arr_Time", r"(\d+):", 1), 2, "0"),
        lpad(regexp_extract("Arr_Time", r":(\d+)", 1), 2, "0")
    ).cast("string"))

    df = df.withColumn("Aircraft", when(trim(col("Aircraft")) == "()", None).otherwise(col("Aircraft"))) \
       .withColumn("Seat_type", when(trim(col("Seat_type")) == "0", None).otherwise(col("Seat_type"))) \
       .withColumn("Flight_class", when(trim(col("Flight_class")) == "0", None).otherwise(col("Flight_class"))) \
       .withColumn("Flight_reason", when(trim(col("Flight_reason")) == "0", None).otherwise(col("Flight_reason")))

    df = df.drop("From", "To", "Airline","Aircraft_id", "Dep_id", "Arr_id", "Airline_id", "_rescued_data","Note")
    return df
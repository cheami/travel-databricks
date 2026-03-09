CREATE OR REFRESH MATERIALIZED VIEW silver.manual_log(
)
COMMENT "Manual log containing travel itinerary for each day"
AS SELECT Day,Date,Country,City,Description,Comments,Food,Travel,Hotel
FROM bronze.manual_log;
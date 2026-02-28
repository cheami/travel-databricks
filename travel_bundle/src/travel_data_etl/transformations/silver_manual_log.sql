-- Define a materialized view that validates data and renames a column
CREATE OR REFRESH MATERIALIZED VIEW silver.manual_log(
--CONSTRAINT valid_artist_name EXPECT (artist_name IS NOT NULL)
)
COMMENT "Manual log containing travel itinerary for each day"
AS SELECT Day,Date,Country,City,Description,Comments,Food,Travel,Hotel
FROM bronze.manual_log;
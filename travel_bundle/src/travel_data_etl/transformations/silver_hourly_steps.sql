CREATE MATERIALIZED VIEW silver.hourly_steps AS
SELECT
  date_trunc('hour', timestamp) AS Start_Hour,
  date_trunc('hour', timestamp) + INTERVAL 1 HOUR AS End_Hour,
  SUM(steps) AS Steps,
  data_source
FROM bronze.fitbit_steps
GROUP BY
  date_trunc('hour', timestamp),
  data_source
ORDER BY start_hour
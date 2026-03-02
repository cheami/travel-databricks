CREATE MATERIALIZED VIEW silver.hourly_steps_and_heart_rate AS
SELECT
  s.Start_Hour,
  s.End_Hour,
  s.Steps,
  h.Heart_Rate,
  s.Data_Source
FROM (
  SELECT
    date_trunc('hour', timestamp) AS Start_Hour,
    date_trunc('hour', timestamp) + INTERVAL 1 HOUR AS End_Hour,
    SUM(steps) AS Steps,
    Data_Source
  FROM bronze.fitbit_steps
  GROUP BY
    date_trunc('hour', timestamp),
    data_source
) s
LEFT JOIN (
  SELECT
    date_trunc('hour', timestamp) AS Start_Hour,
    date_trunc('hour', timestamp) + INTERVAL 1 HOUR AS End_Hour,
    format_number(AVG(beats_per_minute), 2) AS Heart_Rate
  FROM bronze.fitbit_heart_rate
  GROUP BY
    date_trunc('hour', timestamp)
) h
ON s.Start_Hour = h.Start_Hour
WHERE s.Start_Hour >= DATE '2025-02-25'  --Start of World Travel
ORDER BY s.Start_Hour ASC;
{{ config(materialized='incremental', unique_key='Start_Hour') }}

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
  FROM {{ source('bronze', 'fitbit_steps') }}
  GROUP BY
    date_trunc('hour', timestamp),
    data_source
) s
LEFT JOIN (
  SELECT
    date_trunc('hour', timestamp) AS Start_Hour,
    date_trunc('hour', timestamp) + INTERVAL 1 HOUR AS End_Hour,
    format_number(AVG(beats_per_minute), 2) AS Heart_Rate
  FROM {{ source('bronze', 'fitbit_heart_rate') }}
  GROUP BY
    date_trunc('hour', timestamp)
) h
ON s.Start_Hour = h.Start_Hour
WHERE s.Start_Hour >= DATE '2025-02-25'
{% if is_incremental() %}
  AND s.Start_Hour > (SELECT MAX(Start_Hour) FROM {{ this }})
{% endif %}
ORDER BY s.Start_Hour ASC
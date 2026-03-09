{{ config(materialized='incremental', unique_key='Sleep_Log_Entry_Id') }}


SELECT
  sleep_log_entry_id AS Sleep_Log_Entry_Id,
  timestamp AS Timestamp,
  overall_score AS Overall_Score,
  composition_score AS Composition_Score,
  revitalization_score AS Revitalization_Score,
  duration_score AS Duration_Score,
  deep_sleep_in_minutes AS Deep_Sleep_In_Minutes,
  resting_heart_rate AS Resting_Heart_Rate,
  restlessness AS Restlessness,
  CASE
    WHEN overall_score >= 90 THEN 'Excellent'
    WHEN overall_score >= 80 THEN 'Good'
    WHEN overall_score >= 60 THEN 'Fair'
    ELSE 'Poor'
  END AS Sleep_Quality
FROM {{ source('bronze', 'fitbit_sleep_score') }}
WHERE timestamp >= DATE '2025-02-25'
{% if is_incremental() %}
  AND timestamp > (SELECT MAX(Timestamp) FROM {{ this }})
{% endif %}
ORDER BY timestamp ASC
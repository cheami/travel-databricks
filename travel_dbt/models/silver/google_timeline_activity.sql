{{ config(materialized='incremental', unique_key='segment_start_time') }}

SELECT
    segment_start_time,
    segment_end_time,
    start_tz_offset_mins,
    end_tz_offset_mins,
    activity_distance_meters,
    activity_probability,
    activity_type,
    activity_top_candidate_probability,
    activity_start_latlng,
    activity_end_latlng,
    parking_start_time,
    parking_latlng
FROM {{ ref('stg_google_timeline') }}
WHERE activity_type IS NOT NULL
{% if is_incremental() %}
  AND segment_start_time > (SELECT MAX(segment_start_time) FROM {{ this }})
{% endif %}
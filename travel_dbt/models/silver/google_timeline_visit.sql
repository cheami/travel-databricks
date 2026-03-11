{{ config(materialized='incremental', unique_key='segment_start_time') }}

SELECT
    segment_start_time,
    segment_end_time,
    start_tz_offset_mins,
    end_tz_offset_mins,
    visit_hierarchy_level,
    visit_is_timeless,
    visit_probability,
    visit_place_id,
    visit_semantic_type,
    visit_top_candidate_probability,
    visit_latlng,
    trip_distance_from_origin_kms,
    trip_destination_place_id
FROM {{ ref('stg_google_timeline') }}
WHERE visit_place_id IS NOT NULL
{% if is_incremental() %}
  AND segment_start_time > (SELECT MAX(segment_start_time) FROM {{ this }})
{% endif %}
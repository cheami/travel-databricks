--TODO: SPLIT INTO 2 TABLES: 1. ACTIVITY, 2. VISIT
CREATE OR REFRESH MATERIALIZED VIEW silver.google_timeline (
  CONSTRAINT segment_start_time_not_null EXPECT (segment_start_time IS NOT NULL)
)
AS
SELECT
    -- 1. BASE SEGMENT TIMING
    seg.startTime AS segment_start_time,
    seg.endTime AS segment_end_time,
    seg.startTimeTimezoneUtcOffsetMinutes AS start_tz_offset_mins,
    seg.endTimeTimezoneUtcOffsetMinutes AS end_tz_offset_mins,
    -- 2. ACTIVITY DATA (Flattened Structs)
    seg.activity.distanceMeters AS activity_distance_meters,
    seg.activity.probability AS activity_probability,
    seg.activity.topCandidate.type AS activity_type,
    seg.activity.topCandidate.probability AS activity_top_candidate_probability,
    seg.activity.start.latLng AS activity_start_latlng,
    seg.activity.end.latLng AS activity_end_latlng,
    seg.activity.parking.startTime AS parking_start_time,
    seg.activity.parking.location.latLng AS parking_latlng,
    -- 3. VISIT DATA (Flattened Structs)
    seg.visit.hierarchyLevel AS visit_hierarchy_level,
    seg.visit.isTimelessVisit AS visit_is_timeless,
    seg.visit.probability AS visit_probability,
    seg.visit.topCandidate.placeId AS visit_place_id,
    seg.visit.topCandidate.semanticType AS visit_semantic_type,
    seg.visit.topCandidate.probability AS visit_top_candidate_probability,
    seg.visit.topCandidate.placeLocation.latLng AS visit_latlng,
    -- 4. TIMELINE MEMORY / TRIP DATA (Flattened Struct + Exploded Array)
    seg.timelineMemory.trip.distanceFromOriginKms AS trip_distance_from_origin_kms,
    trip_dest.identifier.placeId AS trip_destination_place_id
FROM bronze.google_timeline
LATERAL VIEW EXPLODE_OUTER(raw_json.semanticSegments) exploded_segments AS seg
LATERAL VIEW EXPLODE_OUTER(seg.timelinePath) exploded_paths AS path_point
LATERAL VIEW EXPLODE_OUTER(seg.timelineMemory.trip.destinations) exploded_destinations AS trip_dest
WHERE seg.startTime >= '2025-02-25'
  AND path_point.time IS NULL;



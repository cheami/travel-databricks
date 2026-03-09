{{ config(materialized='incremental', unique_key=['Date', 'Flight_Number']) }}

SELECT
    Date,
    Flight_number,
    -- FROM parsing
    regexp_extract(From, '^([^/]+)', 1)                     AS From_City,
    regexp_extract(From, '/\\s*([^(]+)', 1)                 AS From_Airport,
    regexp_extract(From, '\\(([^/]+)', 1)                   AS From_IATA,
    regexp_extract(From, '\\((?:[^/]+/)?([A-Z0-9]+)\\)', 1) AS From_ICAO,
    -- TO parsing
    regexp_extract(To, '^([^/]+)', 1)                       AS To_City,
    regexp_extract(To, '/\\s*([^(]+)', 1)                   AS To_Airport,
    regexp_extract(To, '\\(([^/]+)', 1)                     AS To_IATA,
    regexp_extract(To, '\\((?:[^/]+/)?([A-Z0-9]+)\\)', 1)  AS To_ICAO,
    -- Airline parsing
    regexp_extract(Airline, '^([^(]+)', 1)                  AS Airline_Name,
    regexp_extract(Airline, '\\(([^/]+)', 1)                AS Airline_IATA,
    regexp_extract(Airline, '/([A-Z0-9]+)\\)', 1)           AS Airline_ICAO,
    -- Time/duration formatting
    concat_ws(':',
        lpad(regexp_extract(Duration, '(\\d+):', 1), 2, '0'),
        lpad(regexp_extract(Duration, ':(\\d+)', 1), 2, '0')
    ) AS Duration,
    concat_ws(':',
        lpad(regexp_extract(Dep_Time, '(\\d+):', 1), 2, '0'),
        lpad(regexp_extract(Dep_Time, ':(\\d+)', 1), 2, '0')
    ) AS Dep_Time,
    concat_ws(':',
        lpad(regexp_extract(Arr_Time, '(\\d+):', 1), 2, '0'),
        lpad(regexp_extract(Arr_Time, ':(\\d+)', 1), 2, '0')
    ) AS Arr_Time,
    -- Nullify placeholder values
    CASE WHEN trim(Aircraft) = '()'  THEN NULL ELSE Aircraft      END AS Aircraft,
    CASE WHEN trim(Seat_type) = '0'  THEN NULL ELSE Seat_type     END AS Seat_type,
    CASE WHEN trim(Flight_class) = '0' THEN NULL ELSE Flight_class END AS Flight_class,
    CASE WHEN trim(Flight_reason) = '0' THEN NULL ELSE Flight_reason END AS Flight_reason
FROM {{ source('bronze', 'flight_log') }}
WHERE Date >= DATE '2025-02-25'
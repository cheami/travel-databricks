{{ config(materialized='incremental', unique_key='Date') }}

SELECT
  Day,
  Date,
  Country,
  City,
  Description,
  Comments,
  Food,
  Travel,
  Hotel
FROM {{ source('bronze', 'manual_log') }}
{% if is_incremental() %}
  WHERE Date > (SELECT MAX(Date) FROM {{ this }})
{% endif %}
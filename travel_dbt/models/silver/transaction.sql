{{ config(materialized='table') }}

SELECT
  Country,
  Date,
  Name,
  Type,
  Amount,
  Comments
FROM {{ source('bronze', 'transaction') }}
WHERE Date >= DATE '2025-02-25'
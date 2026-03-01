CREATE MATERIALIZED VIEW gold.full_travel_cost AS
SELECT
  ml.Day,
  ml.Date,
  ml.Country,
  ml.City,
  ml.Description,
  ml.Comments,
  ml.Food,
  ml.Travel,
  ml.Hotel,
  format_number(SUM(CASE WHEN t.Type = 'Hotel' THEN try_cast(regexp_replace(t.Amount, '[$,]', '') AS DOUBLE) ELSE 0 END), 2) AS Hotel_Total,
  format_number(SUM(CASE WHEN t.Type = 'Food' THEN try_cast(regexp_replace(t.Amount, '[$,]', '') AS DOUBLE) ELSE 0 END), 2) AS Food_Total,
  format_number(SUM(CASE WHEN t.Type = 'Travel' THEN try_cast(regexp_replace(t.Amount, '[$,]', '') AS DOUBLE) ELSE 0 END), 2) AS Travel_Total,
  format_number(SUM(CASE WHEN t.Type = 'Activity' THEN try_cast(regexp_replace(t.Amount, '[$,]', '') AS DOUBLE) ELSE 0 END), 2) AS Activity_Total,
  format_number(SUM(CASE WHEN t.Type = 'Misc' THEN try_cast(regexp_replace(t.Amount, '[$,]', '') AS DOUBLE) ELSE 0 END), 2) AS Misc_Total,
  format_number(SUM(try_cast(regexp_replace(t.Amount, '[$,]', '') AS DOUBLE)), 2) AS Daily_Total,
  format_number(SUM(SUM(try_cast(regexp_replace(t.Amount, '[$,]', '') AS DOUBLE))) OVER (ORDER BY ml.Date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2) AS Running_Total_Amount,
  format_number(AVG(SUM(try_cast(regexp_replace(t.Amount, '[$,]', '') AS DOUBLE))) OVER (ORDER BY ml.Date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2) AS Running_Daily_Average
FROM dev.silver.manual_log ml
LEFT JOIN dev.silver.transaction t ON ml.Date = t.Date
GROUP BY
  ml.Day,
  ml.Date,
  ml.Country,
  ml.City,
  ml.Description,
  ml.Comments,
  ml.Food,
  ml.Travel,
  ml.Hotel
ORDER BY ml.Date ASC;
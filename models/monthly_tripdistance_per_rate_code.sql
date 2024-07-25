{{ config(materialized='table') }}

WITH trip_data AS (
  SELECT
    RatecodeID,
    {{ year_extract('lpep_pickup_datetime') }} AS year,
    CASE
      WHEN {{ month_extract('lpep_pickup_datetime') }} = 1 THEN 'January'
      WHEN {{ month_extract('lpep_pickup_datetime') }} = 2 THEN 'February'
      WHEN {{ month_extract('lpep_pickup_datetime') }} = 3 THEN 'March'
      WHEN {{ month_extract('lpep_pickup_datetime') }} = 4 THEN 'April'
      WHEN {{ month_extract('lpep_pickup_datetime') }} = 5 THEN 'May'
      WHEN {{ month_extract('lpep_pickup_datetime') }} = 6 THEN 'June'
      WHEN {{ month_extract('lpep_pickup_datetime') }} = 7 THEN 'July'
      WHEN {{ month_extract('lpep_pickup_datetime') }} = 8 THEN 'August'
      WHEN {{ month_extract('lpep_pickup_datetime') }} = 9 THEN 'September'
      WHEN {{ month_extract('lpep_pickup_datetime') }} = 10 THEN 'October'
      WHEN {{ month_extract('lpep_pickup_datetime') }} = 11 THEN 'November'
      WHEN {{ month_extract('lpep_pickup_datetime') }} = 12 THEN 'December'
    END AS month,
    SUM(trip_distance) AS total_trip_distance,
    AVG(trip_distance) AS avg_trip_distance
  FROM
    {{ source('week3_dbt', 'nyc_taxi_trip') }}
  GROUP BY
    year,
    month,
    RatecodeID
  ORDER BY
    year,
    month,
    RatecodeID
)

SELECT
  RatecodeID,
  year,
  month,
  total_trip_distance,
  avg_trip_distance
FROM
  trip_data
ORDER BY
  year,
  month,
  RatecodeID

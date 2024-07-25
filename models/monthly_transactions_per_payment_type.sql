{{ config(materialized='table') }}

WITH month_total AS (
  SELECT
    payment_type,
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
    COUNT(*) AS transaction_count,
    SUM(total_amount) AS total_transactions,
    AVG(total_amount) AS avg_transaction_amount
  FROM
    {{ source('week3_dbt', 'nyc_taxi_trip') }}
  GROUP BY 
    year,
    month,
    payment_type
  ORDER BY
    year,
    month,
    payment_type
)

SELECT 
  payment_type,
  year, 
  month,
  transaction_count,
  total_transactions,
  {{ convert_idr('total_transactions') }} AS total_transactions_idr,
  avg_transaction_amount,
  {{ convert_idr('avg_transaction_amount') }} AS avg_transaction_amount_idr

FROM 
  month_total

ORDER BY
  year,
  month,
  payment_type ASC

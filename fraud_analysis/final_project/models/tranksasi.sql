-- file: models/transformations.sql

-- Jika Anda ingin menggunakan view
{{ config(
  materialized='view'
) }}

-- Transformasi data dari pandas
WITH transformed_data AS (
  SELECT
    payment_datetime,
    isFraud,
    payment_id,
    amount,
    type,
    EXTRACT(HOUR FROM payment_datetime) AS hour_of_day,
    CASE WHEN EXTRACT(HOUR FROM payment_datetime) BETWEEN 7 AND 18 THEN 'siang' ELSE 'malam' END AS waktu,
    EXTRACT(DAY FROM payment_datetime) AS day_of_month,
    CASE 
      WHEN EXTRACT(DAY FROM payment_datetime) BETWEEN 1 AND 7 THEN 1
      WHEN EXTRACT(DAY FROM payment_datetime) BETWEEN 8 AND 14 THEN 2
      WHEN EXTRACT(DAY FROM payment_datetime) BETWEEN 15 AND 21 THEN 3
      WHEN EXTRACT(DAY FROM payment_datetime) BETWEEN 22 AND 28 THEN 4
      ELSE 5
    END AS minggu_ke,
    EXTRACT(DAY FROM payment_datetime) AS hari_ke
  FROM
    {{ source('Finalproject', 'Fraud') }}
)

-- Pilih kolom yang Anda butuhkan
SELECT
  payment_datetime,
  isFraud,
  payment_id,
  amount,
  type,
  hour_of_day,
  waktu,
  day_of_month,
  minggu_ke,
  hari_ke
FROM transformed_data

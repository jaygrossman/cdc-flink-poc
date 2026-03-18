{{
  config(
    materialized='incremental',
    unique_key=['policy_id', 'vehicle_vin', 'license_number'],
    incremental_strategy='delete+insert',
    post_hook=[
      "DELETE FROM {{ this }} WHERE (policy_id, vehicle_vin, license_number) IN (SELECT s.policy_id, s.vehicle_vin, s.license_number FROM {{ source('cdc', 'stg_driver') }} s INNER JOIN (SELECT policy_id, vehicle_vin, license_number, MAX(stg_id) AS max_id FROM {{ source('cdc', 'stg_driver') }} GROUP BY policy_id, vehicle_vin, license_number) m ON s.policy_id = m.policy_id AND s.vehicle_vin = m.vehicle_vin AND s.license_number = m.license_number AND s.stg_id = m.max_id WHERE s.op = 'd')"
    ]
  )
}}

WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY policy_id, vehicle_vin, license_number
               ORDER BY event_time DESC, stg_id DESC
           ) AS rn
    FROM {{ source('cdc', 'stg_driver') }}
),

latest AS (
    SELECT * FROM ranked WHERE rn = 1
)

SELECT
    policy_id,
    vehicle_vin,
    driver_name,
    license_number,
    is_primary
FROM latest
WHERE op != 'd'

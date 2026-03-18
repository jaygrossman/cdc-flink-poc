

WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY policy_id, vehicle_vin, license_number
               ORDER BY event_time DESC, stg_id DESC
           ) AS rn
    FROM "cdc_db"."public"."stg_driver"
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
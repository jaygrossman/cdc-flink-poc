

WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY policy_id, vin
               ORDER BY event_time DESC, stg_id DESC
           ) AS rn
    FROM "cdc_db"."public"."stg_vehicle"
),

latest AS (
    SELECT * FROM ranked WHERE rn = 1
)

SELECT
    policy_id,
    vin,
    year_made,
    make,
    model
FROM latest
WHERE op != 'd'


WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY policy_id, coverage_type
               ORDER BY event_time DESC, stg_id DESC
           ) AS rn
    FROM "cdc_db"."public"."stg_coverage"
),

latest AS (
    SELECT * FROM ranked WHERE rn = 1
)

SELECT
    policy_id,
    coverage_type,
    coverage_limit,
    deductible,
    premium
FROM latest
WHERE op != 'd'
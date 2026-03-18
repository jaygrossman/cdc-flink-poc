

WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY policy_id, claim_id
               ORDER BY event_time DESC, stg_id DESC
           ) AS rn
    FROM "cdc_db"."public"."stg_claim"
),

latest AS (
    SELECT * FROM ranked WHERE rn = 1
)

SELECT
    policy_id,
    claim_id,
    claim_date,
    amount,
    status,
    description
FROM latest
WHERE op != 'd'
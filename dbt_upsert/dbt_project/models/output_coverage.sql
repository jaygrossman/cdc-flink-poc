{{
  config(
    materialized='incremental',
    unique_key=['policy_id', 'coverage_type'],
    incremental_strategy='delete+insert',
    post_hook=[
      "DELETE FROM {{ this }} WHERE (policy_id, coverage_type) IN (SELECT s.policy_id, s.coverage_type FROM {{ source('cdc', 'stg_coverage') }} s INNER JOIN (SELECT policy_id, coverage_type, MAX(stg_id) AS max_id FROM {{ source('cdc', 'stg_coverage') }} GROUP BY policy_id, coverage_type) m ON s.policy_id = m.policy_id AND s.coverage_type = m.coverage_type AND s.stg_id = m.max_id WHERE s.op = 'd')"
    ]
  )
}}

WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY policy_id, coverage_type
               ORDER BY event_time DESC, stg_id DESC
           ) AS rn
    FROM {{ source('cdc', 'stg_coverage') }}
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

{{
  config(
    materialized='incremental',
    unique_key=['policy_id', 'claim_id'],
    incremental_strategy='delete+insert',
    post_hook=[
      "DELETE FROM {{ this }} WHERE (policy_id, claim_id) IN (SELECT s.policy_id, s.claim_id FROM {{ source('cdc', 'stg_claim') }} s INNER JOIN (SELECT policy_id, claim_id, MAX(stg_id) AS max_id FROM {{ source('cdc', 'stg_claim') }} GROUP BY policy_id, claim_id) m ON s.policy_id = m.policy_id AND s.claim_id = m.claim_id AND s.stg_id = m.max_id WHERE s.op = 'd')"
    ]
  )
}}

WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY policy_id, claim_id
               ORDER BY event_time DESC, stg_id DESC
           ) AS rn
    FROM {{ source('cdc', 'stg_claim') }}
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

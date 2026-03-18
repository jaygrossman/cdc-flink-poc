{{
  config(
    materialized='incremental',
    unique_key=['policy_id', 'vin'],
    incremental_strategy='delete+insert',
    post_hook=[
      "DELETE FROM {{ this }} WHERE (policy_id, vin) IN (SELECT s.policy_id, s.vin FROM {{ source('cdc', 'stg_vehicle') }} s INNER JOIN (SELECT policy_id, vin, MAX(stg_id) AS max_id FROM {{ source('cdc', 'stg_vehicle') }} GROUP BY policy_id, vin) m ON s.policy_id = m.policy_id AND s.vin = m.vin AND s.stg_id = m.max_id WHERE s.op = 'd')"
    ]
  )
}}

WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY policy_id, vin
               ORDER BY event_time DESC, stg_id DESC
           ) AS rn
    FROM {{ source('cdc', 'stg_vehicle') }}
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

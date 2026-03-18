{{
  config(
    materialized='incremental',
    unique_key='policy_id',
    incremental_strategy='delete+insert',
    post_hook=[
      "DELETE FROM {{ this }} WHERE policy_id IN (SELECT s.policy_id FROM {{ source('cdc', 'stg_policy') }} s WHERE s.op = 'd' AND s.stg_id = (SELECT MAX(s2.stg_id) FROM {{ source('cdc', 'stg_policy') }} s2 WHERE s2.policy_id = s.policy_id))"
    ]
  )
}}

WITH new_events AS (
    SELECT *
    FROM {{ source('cdc', 'stg_policy') }}
    {% if is_incremental() %}
    WHERE event_time >= (SELECT COALESCE(MAX(source_event_time), '1970-01-01'::timestamp) FROM {{ this }})
    {% endif %}
),

ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY policy_id
               ORDER BY event_time DESC, stg_id DESC
           ) AS rn
    FROM new_events
),

latest AS (
    SELECT * FROM ranked WHERE rn = 1
)

SELECT
    policy_id,
    policy_number,
    status,
    effective_date,
    expiration_date,
    holder_first_name,
    holder_last_name,
    holder_dob,
    holder_email,
    holder_phone,
    holder_street,
    holder_city,
    holder_state,
    holder_zip,
    source_event_time
FROM latest
WHERE op != 'd'

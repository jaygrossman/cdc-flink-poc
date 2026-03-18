

WITH new_events AS (
    SELECT *
    FROM "cdc_db"."public"."stg_policy"
    
    WHERE event_time >= (SELECT COALESCE(MAX(source_event_time), '1970-01-01'::timestamp) FROM "cdc_db"."public"."output_policy")
    
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
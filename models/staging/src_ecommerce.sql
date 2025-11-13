{{ config(
    materialized='incremental',
    unique_key='event_key'
) }}

WITH raw_data AS (
    SELECT * FROM ecommerce.raw.raw_ecommerce_nov
    UNION ALL
    SELECT * FROM ecommerce.raw.raw_ecommerce_dec
)

SELECT
    CAST(TRY_TO_TIMESTAMP_NTZ(LEFT(event_time, 19)) AS TIMESTAMP_NTZ) AS event_time_ts,
    CAST(TRY_TO_TIMESTAMP_NTZ(LEFT(event_time, 19)) AS DATE) AS event_date,
    CAST(TRY_TO_TIMESTAMP_NTZ(LEFT(event_time, 19)) AS TIME) AS event_time_only,
    event_type,
    product_id,
    category_id,
    CASE 
        WHEN POSITION('.' IN category_code) > 0
            THEN SPLIT_PART(category_code, '.', 1)
        ELSE 'UNKNOWN'
    END AS category,
    CASE 
        WHEN POSITION('.' IN category_code) > 0
            THEN SPLIT_PART(category_code, '.', 2)
        ELSE 'UNKNOWN'
    END AS subcategory,
    COALESCE(brand, 'UNKNOWN') AS brand,
    price,
    user_id,
    COALESCE(user_session, 'UNKNOWN') AS user_session,

    -- Independent unique key for incremental loads
    UUID_STRING() AS event_key

FROM raw_data

{% if is_incremental() %}
-- Only insert rows that are new based on surrogate key
WHERE event_key NOT IN (SELECT event_key FROM {{ this }})
{% endif %}

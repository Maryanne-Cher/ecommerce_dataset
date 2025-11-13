{{ config(
    materialized='incremental',
    unique_key='event_key'
) }}

WITH ecommerce_behavior AS (
    SELECT *
    FROM {{ ref('src_ecommerce') }}
),

fact AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY event_time_ts ASC) AS event_key,
        event_time_ts,
        event_date,
        event_time_only,
        event_type,
        product_id,
        category_id,
        category,
        subcategory,
        price,
        user_id,
        user_session
    FROM ecommerce_behavior
)

SELECT *
FROM fact
{% if is_incremental() %}
  WHERE event_key NOT IN (SELECT event_key FROM {{ this }})
{% endif %}

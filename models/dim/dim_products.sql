{{ config(
    materialized='incremental',
    unique_key='product_id'
) }}

WITH ecommerce_behavior AS (
    SELECT *
    FROM {{ ref('src_ecommerce') }}
),

ranked_products AS (
    SELECT
        product_id,
        category_id,
        brand,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY category_id, brand  -- optional: consider event_time_ts for latest
        ) AS rn
    FROM ecommerce_behavior
)

SELECT
    product_id,
    category_id,
    brand
FROM ranked_products
WHERE rn = 1
{% if is_incremental() %}
  AND product_id NOT IN (SELECT product_id FROM {{ this }})
{% endif %}

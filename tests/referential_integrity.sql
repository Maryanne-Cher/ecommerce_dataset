-- tests/assert_referential_integrity_fact_products.sql
-- This test checks referential integrity between fact_ecommerce and dim_products

SELECT f.product_id
FROM {{ ref('fact_ecommerce') }} AS f
LEFT JOIN {{ ref('dim_products') }} AS p
    ON f.product_id = p.product_id
WHERE p.product_id IS NULL

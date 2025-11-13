SELECT
price
FROM {{ ref('fact_ecommerce')}}
WHERE price <=0
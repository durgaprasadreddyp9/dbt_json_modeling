{{ config(materialized='table') }}

SELECT
  {{ dbt_utils.generate_surrogate_key(['sale.value:sale_id']) }} AS sales_sk,
  sale.value:sale_id::STRING        AS sale_id,
  sale.value:order_id::STRING       AS order_id,
  sale.value:product_id::STRING     AS product_id,
  sale.value:product_name::STRING   AS product_name,
  sale.value:quantity::NUMBER       AS quantity,
  sale.value:unit_price::FLOAT      AS unit_price,
  sale.value:total_revenue::FLOAT   AS total_revenue,
  sale.value:sale_date::DATE        AS sale_date,
  sale.value:is_returned::BOOLEAN   AS is_returned,
  sale.value:return_date::DATE      AS return_date,

  OBJECT_CONSTRUCT(
    'sale_id', sale.value:sale_id,
    'order_id', sale.value:order_id,
    'product_id', sale.value:product_id,
    'product_name', sale.value:product_name,
    'quantity', sale.value:quantity,
    'unit_price', sale.value:unit_price,
    'total_revenue', sale.value:total_revenue,
    'sale_date', sale.value:sale_date,
    'is_returned', sale.value:is_returned,
    'return_date', sale.value:return_date
  ) AS sale_object,

  CURRENT_TIMESTAMP() AS updated_at

FROM {{ ref('src_sales') }},
     LATERAL FLATTEN(input => data) AS sale

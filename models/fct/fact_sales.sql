{{ config(materialized='table') }}

SELECT
  {{ dbt_utils.generate_surrogate_key(['s.sale_id', 's.order_id', 'c.customer_id']) }} AS fct_sales_sk,
  s.sales_sk,
  o.orders_sk,
  c.customer_sk,

  s.sale_id,
  s.sale_date,
  s.product_id,
  s.product_name,
  s.quantity,
  s.unit_price,
  s.total_revenue,
  s.is_returned,
  s.return_date,

  o.order_id,
  o.order_date,
  o.order_status,
  o.shipping_address,

  c.customer_id,
  c.customer_name,
  c.email,
  c.phone,
  c.city,
  c.state,
  c.zip_code,

  CURRENT_TIMESTAMP() AS updated_at

FROM {{ ref('dim_sales') }} s
LEFT JOIN {{ ref('dim_orders') }} o ON s.order_id = o.order_id
LEFT JOIN {{ ref('dim_customers') }} c ON o.customer_id = c.customer_id

{{ config(materialized='table') }}

SELECT
  {{ dbt_utils.generate_surrogate_key(['order_rec.value:order_id']) }} AS orders_sk,
  order_rec.value:order_id::STRING         AS order_id,
  order_rec.value:customer_id::STRING      AS customer_id,
  order_rec.value:order_date::DATE         AS order_date,
  order_rec.value:order_status::STRING     AS order_status,
  order_rec.value:shipping_address::STRING AS shipping_address,
  item.value:product_id::STRING            AS product_id,
  item.value:quantity::NUMBER              AS quantity,
  item.value:price::FLOAT                  AS price,

  OBJECT_CONSTRUCT(
    'order_id', order_rec.value:order_id,
    'customer_id', order_rec.value:customer_id,
    'order_date', order_rec.value:order_date,
    'order_status', order_rec.value:order_status,
    'shipping_address', order_rec.value:shipping_address,
    'product_id', item.value:product_id,
    'quantity', item.value:quantity,
    'price', item.value:price
  ) AS order_object,

  CURRENT_TIMESTAMP() AS updated_at

FROM {{ source('raw', 'RAW_ORDERS') }} AS raw_orders,
     LATERAL FLATTEN(input => raw_orders.data) AS order_rec,
     LATERAL FLATTEN(input => order_rec.value:order_details) AS item

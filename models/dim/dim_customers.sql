{{ config(materialized='table') }}

SELECT
  {{ dbt_utils.generate_surrogate_key(['customer.value:customer_id']) }} AS customer_sk,
  customer.value:customer_id::STRING        AS customer_id,
  customer.value:customer_name::STRING      AS customer_name,
  customer.value:contact_info:email::STRING AS email,
  customer.value:contact_info:phone::STRING AS phone,
  customer.value:address:street::STRING     AS street,
  customer.value:address:city::STRING       AS city,
  customer.value:address:state::STRING      AS state,
  customer.value:address:zip_code::STRING   AS zip_code,

  OBJECT_CONSTRUCT(
    'customer_id', customer.value:customer_id,
    'customer_name', customer.value:customer_name,
    'email', customer.value:contact_info:email,
    'phone', customer.value:contact_info:phone,
    'street', customer.value:address:street,
    'city', customer.value:address:city,
    'state', customer.value:address:state,
    'zip_code', customer.value:address:zip_code
  ) AS customer_object,

  CURRENT_TIMESTAMP() AS updated_at

FROM {{ source('raw', 'RAW_CUSTOMERS') }},
     LATERAL FLATTEN(input => data) AS customer

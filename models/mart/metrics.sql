{{ config(materialized='table') }}

SELECT *
FROM fact_sales
where sale_date BETWEEN DATEADD(day, -3, DATE '2025-07-25') and DATE '2025-07-25'
ORDER BY sale_date DESC
/* Task: query_redshift
  Source: dags/astro-example-dags/sqls/revenue_query.sql
*/
SELECT
    order_id,
    customer_id,
    product_sku,
    quantity,
    line_total,
    status,
    country_code,
    ingested_at
FROM public.raw_orders
WHERE ingested_at::date = '{{ data_interval_start | ds }}'
  AND status != 'cancelled'
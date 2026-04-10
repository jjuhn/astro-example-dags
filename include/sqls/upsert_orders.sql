-- sqls/upsert_orders.sql
BEGIN;
CREATE TEMP TABLE stage_orders (LIKE {{ target_table }});

{% for order in orders %}
INSERT INTO stage_orders VALUES (
    '{{ order.order_id }}', '{{ order.customer_id }}', '{{ order.product_sku }}',
    {{ order.quantity }}, {{ order.unit_price }}, '{{ order.status }}', '{{ order.created_at }}'
);
{% endfor %}

DELETE FROM {{ target_table }}
USING stage_orders
WHERE {{ target_table }}.order_id = stage_orders.order_id;

INSERT INTO {{ target_table }} SELECT * FROM stage_orders;
COMMIT;
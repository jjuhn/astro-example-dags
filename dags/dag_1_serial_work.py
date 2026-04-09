from __future__ import annotations

import os
import json
import logging
from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

log = logging.getLogger(__name__)

# --- 1. DYNAMIC PATH DISCOVERY ---
# This eliminates the /usr/local/airflow hardcoding
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
SQL_DIR = os.path.join(DAG_DIR, 'sqls')

# --- 2. CONFIGURATION ---
REDSHIFT_CONN = Variable.get("redshift_conn", default_var="redshift_default")
TARGET_TABLE = "public.raw_orders"

@dag(
    dag_id="dag1_serial_order_etl_redshift",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    # 3. Use the dynamic path here
    template_searchpath=[SQL_DIR],
    tags=["redshift"]
)
def production_serial_etl():

    @task
    def extract_orders():
        """Simulates an API call to an e-commerce backend."""
        return [
            {"order_id": "ORD-101", "customer_id": "CUST-01", "product_sku": "SKU-A", "quantity": 2, "unit_price": 50.0, "status": "shipped", "created_at": "2026-04-09"},
            {"order_id": "ORD-102", "customer_id": "CUST-02", "product_sku": "SKU-B", "quantity": 1, "unit_price": 120.0, "status": "pending", "created_at": "2026-04-09"}
        ]

    @task
    def validate_schema(orders: list):
        """Validates data quality before transformation."""
        required_fields = ["order_id", "quantity", "unit_price"]
        valid_orders = [o for o in orders if all(k in o for k in required_fields) and o['quantity'] > 0]

        if not valid_orders:
            raise ValueError("No valid orders found in batch!")
        return valid_orders

    @task
    def transform_orders(orders: list):
        """Applies business logic (Line Total & Masking)."""
        for order in orders:
            order["line_total"] = round(order["quantity"] * order["unit_price"], 2)
            order["customer_id"] = f"***{order['customer_id'][-2:]}"
        return orders

    @task
    def load_to_redshift(orders: list, **context):
        """
        TASK: Load
        Renders the upsert SQL from the /sqls folder and logs it.
        """
        # 4. Use the DAG's environment to find the template
        jinja_env = context['dag'].get_template_env()
        template = jinja_env.get_template("upsert_orders.sql")

        rendered_sql = template.render(
            target_table=TARGET_TABLE,
            orders=orders,
            **context
        )

        log.info("--- PREVIEW: RENDERED UPSERT SQL ---")
        log.info(rendered_sql)
        log.info("--- END PREVIEW ---")

        # hook = PostgresHook(postgres_conn_id=REDSHIFT_CONN)
        # hook.run(rendered_sql)

    # Dependencies
    processed_orders = transform_orders(validate_schema(extract_orders()))
    load_to_redshift(processed_orders)

production_serial_etl()
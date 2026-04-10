from __future__ import annotations

import os
import logging
from datetime import datetime

from airflow.sdk import dag, task, Asset
from airflow.providers.postgres.hooks.postgres import PostgresHook
from include.assets.constants import ENRICHED_ORDERS, PROCESSED_ORDERS

log = logging.getLogger(__name__)

# =============================================================================
# DAG 1: Serial Order ETL → Redshift (Lab Requirement #1)
# =============================================================================
# Purpose: Classic serial pipeline (extract → validate → transform → load)
# Uses shared Astronomer connection "redshift_default"
# Triggers on ENRICHED_ORDERS Asset (data lineage demo)


@dag(
    dag_id="dag1_serial_order_etl_redshift",
    schedule=[ENRICHED_ORDERS],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    template_searchpath=["/usr/local/airflow/include/sqls"],
    tags=["serial", "redshift", "etl", "shared-connection"],
)
def production_serial_etl():
    """
    Serial DAG - 4 tasks in strict sequence.
    Demonstrates classic ETL pattern with Airflow TaskFlow API + shared connection.
    """

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

    # @task(queue="etl-load-queue", outlets=[PROCESSED_ORDERS])   # <-- Astro feature: custom worker queue
    @task(outlets=[PROCESSED_ORDERS])
    def load_to_redshift(orders: list, **context):
        """
        Load task using shared Redshift connection (created in Astronomer UI).
        Uses Jinja template from include/sqls/upsert_orders.sql
        """
        jinja_env = context['dag'].get_template_env()
        template = jinja_env.get_template("upsert_orders.sql")

        rendered_sql = template.render(
            target_table="public.raw_orders",
            orders=orders,
            **context
        )

        log.info("--- PREVIEW: RENDERED UPSERT SQL ---")
        log.info(rendered_sql)
        log.info("--- END PREVIEW ---")

        # Execute using the shared Astronomer-managed connection
        # hook = PostgresHook(postgres_conn_id="redshift_default")
        # hook.run(rendered_sql)

    # Serial dependencies
    processed_orders = transform_orders(validate_schema(extract_orders()))
    load_to_redshift(processed_orders)


production_serial_etl()
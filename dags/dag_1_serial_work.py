"""
DAG 1 — Serial Order ETL
========================
4 tasks that are strictly serially dependent.
Simulates extracting orders from an API, validating the schema,
transforming the data, and loading it into Amazon Redshift.

External connection: Redshift (connection id = 'redshift_default')
"""

from __future__ import annotations

import json
import logging
import random
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

# Updated to Redshift connection ID
REDSHIFT_CONN_ID = "redshift_default"

DEFAULT_ARGS = {
    "owner": "some e commerce data team",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}


@dag(
    dag_id="dag1_serial_order_etl_redshift",
    description="Serial pipeline: extract → validate → transform → load to Redshift",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ecommerce", "etl", "redshift", "serial"],
)
def serial_order_etl():
    """
    ### Serial Order ETL Pipeline

    Extracts raw orders from a simulated API, validates the schema,
    applies business transformations, and loads the results into Redshift.
    """

    @task()
    def extract_orders() -> list[dict]:
        log.info("Extracting orders from e-commerce API...")
        statuses = ["pending", "confirmed", "shipped", "delivered", "cancelled"]
        raw_orders = [
            {
                "order_id": f"ORD-{1000 + i}",
                "customer_id": f"CUST-{random.randint(100, 999)}",
                "product_sku": random.choice(["SKU-A", "SKU-B", "SKU-C", "SKU-D"]),
                "quantity": random.randint(1, 10),
                "unit_price": round(random.uniform(9.99, 299.99), 2),
                "status": random.choice(statuses),
                "created_at": datetime.utcnow().isoformat(),
                "country_code": random.choice(["US", "CA", "GB", "DE", "AU"]),
            }
            for i in range(20)
        ]
        log.info("Extracted %d orders.", len(raw_orders))
        return raw_orders

    @task()
    def validate_schema(orders: list[dict]) -> list[dict]:
        required_fields = {
            "order_id", "customer_id", "product_sku",
            "quantity", "unit_price", "status", "created_at",
        }
        valid_orders = []
        dropped = 0

        for order in orders:
            missing = required_fields - order.keys()
            if missing:
                log.warning("Order %s missing fields: %s — dropping.", order.get("order_id"), missing)
                dropped += 1
                continue
            if order["quantity"] <= 0 or order["unit_price"] <= 0:
                log.warning("Order %s has invalid quantity/price — dropping.", order["order_id"])
                dropped += 1
                continue
            valid_orders.append(order)

        log.info("Validation complete. Valid: %d | Dropped: %d", len(valid_orders), dropped)
        return valid_orders

    @task()
    def transform_orders(orders: list[dict]) -> list[dict]:
        transformed = []
        for order in orders:
            transformed.append(
                {
                    **order,
                    "line_total": round(order["quantity"] * order["unit_price"], 2),
                    "country_code": order.get("country_code", "XX").upper(),
                    "customer_id_masked": "***" + order["customer_id"][-3:],
                    "ingested_at": datetime.utcnow().isoformat(),
                    "status_category": (
                        "active" if order["status"] in ("pending", "confirmed", "shipped")
                        else "terminal"
                    ),
                }
            )
        return transformed

    @task()
    def load_to_redshift(orders: list[dict]) -> None:
        """
        Load transformed orders into Redshift.
        Uses PostgresHook (standard for Redshift).
        Target table: public.raw_orders
        """
        # Redshift uses the PostgresHook
        hook = PostgresHook(postgres_conn_id=REDSHIFT_CONN_ID)

        log.info(
            "Connected to Redshift. Loading %d records into public.raw_orders...",
            len(orders),
        )

        for order in orders[:3]:
            log.info("Sample row: %s", json.dumps(order))

        # Redshift Upsert Strategy:
        # 1. Insert into temp table
        # 2. Delete matching records from target
        # 3. Insert from temp to target
        sample_sql = """
        BEGIN;
        CREATE TEMP TABLE stage_orders (LIKE public.raw_orders);

        -- (In reality, you would use 'copy' or 'insert' to fill the temp table)

        DELETE FROM public.raw_orders 
        USING stage_orders 
        WHERE public.raw_orders.order_id = stage_orders.order_id;

        INSERT INTO public.raw_orders SELECT * FROM stage_orders;
        COMMIT;
        """
        log.info("Typical Redshift Upsert Pattern:\n%s", sample_sql)

        # To execute for real:
        # hook.run(list_of_sql_strings)

        log.info("Load complete. %d rows processed.", len(orders))

    # ── Wire up the serial chain ──────────────────────────────────────────────
    raw = extract_orders()
    validated = validate_schema(raw)
    transformed = transform_orders(validated)
    load_to_redshift(transformed)


serial_order_etl()
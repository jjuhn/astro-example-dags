from __future__ import annotations

import logging
import random
from datetime import datetime, timedelta

from airflow.sdk import dag, task, TaskGroup, Asset
from assets import ENRICHED_ORDERS


log = logging.getLogger(__name__)


@dag(
    dag_id="dag2_parallel_order_enrichment",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["parallel"],
)
def parallel_order_enrichment():
    @task()
    def fetch_raw_order() -> dict:
        return {
            "order_id": f"ORD-{random.randint(2000, 9999)}",
            "customer_id": f"CUST-{random.randint(100, 999)}",
            "product_sku": "SKU-A",
            "shipping_address": {"country": "US"},
            "amount": 150.0,
        }

    # We fetch the order here
    raw_order_obj = fetch_raw_order()

    with TaskGroup(group_id="enrichment_tasks") as enrichment_group:
        @task()
        def enrich_customer(order: dict) -> dict:
            return {"customer_id": order["customer_id"], "tier": "platinum"}

        @task()
        def enrich_product(order: dict) -> dict:
            return {"sku": order["product_sku"], "name": "Headphones"}

        @task()
        def enrich_geo(order: dict) -> dict:
            return {"country": order["shipping_address"]["country"]}

        @task()
        def check_fraud_flag(order: dict) -> dict:
            return {"fraud_score": 0.05, "review_required": False}

        c_data = enrich_customer(raw_order_obj)
        p_data = enrich_product(raw_order_obj)
        g_data = enrich_geo(raw_order_obj)
        f_data = check_fraud_flag(raw_order_obj)

    @task(outlets=[ENRICHED_ORDERS])  # Tell Airflow this task updates the asset
    def merge_and_publish(order: dict, customer: dict, product: dict, geo: dict, fraud: dict) -> None:
        final_record = {
            "order_id": order["order_id"],
            "enrichment": {"customer": customer, "product": product, "geo": geo, "fraud": fraud}
        }
        log.info(f"Successfully enriched order: {order['order_id']}")
        log.info(final_record)

    # Final Fan-in: merge_and_publish will wait for all upstream tasks to finish
    merge_and_publish(raw_order_obj, c_data, p_data, g_data, f_data)


parallel_order_enrichment()
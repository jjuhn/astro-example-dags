from __future__ import annotations

import logging
import random
from datetime import datetime

from airflow.sdk import dag, task, TaskGroup, Asset
from include.assets.constants import ENRICHED_ORDERS

log = logging.getLogger(__name__)

# =============================================================================
# DAG 2: Parallel Order Enrichment (Lab Requirement #2)
# =============================================================================
# Purpose: Demonstrates parallel execution with TaskGroup + fan-in
# Shows how to enrich data in parallel before merging


@dag(
    dag_id="dag2_parallel_order_enrichment",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["parallel", "enrichment", "taskgroup", "asset"],
)
def parallel_order_enrichment():
    """
    Parallel DAG - 4 independent enrichment tasks run concurrently.
    Uses TaskGroup for visual clarity in Graph View.
    """

    @task()
    def fetch_raw_order() -> dict:
        """Simulates fetching a raw order from an e-commerce API."""
        return {
            "order_id": f"ORD-{random.randint(2000, 9999)}",
            "customer_id": f"CUST-{random.randint(100, 999)}",
            "product_sku": "SKU-A",
            "shipping_address": {"country": "US"},
            "amount": 150.0,
        }

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

    # @task(queue="enrichment-queue", outlets=[ENRICHED_ORDERS])   # <-- Astro feature: custom worker queue
    @task(outlets=[ENRICHED_ORDERS])
    def merge_and_publish(order: dict, customer: dict, product: dict, geo: dict, fraud: dict) -> None:
        """
        Fan-in task: merges all parallel enrichments and publishes the final enriched order.
        Runs on a dedicated worker queue (configured in Astro Deployment settings).
        """
        final_record = {
            "order_id": order["order_id"],
            "enrichment": {"customer": customer, "product": product, "geo": geo, "fraud": fraud}
        }
        log.info(f"Successfully enriched order: {order['order_id']}")
        log.info(final_record)

    # Final fan-in
    merge_and_publish(raw_order_obj, c_data, p_data, g_data, f_data)


parallel_order_enrichment()
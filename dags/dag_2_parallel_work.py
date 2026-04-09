"""
DAG 2 — Parallel Order Enrichment
===================================
4 independent enrichment tasks that run concurrently (fan-out),
then merge into a single publish task (fan-in).

Demonstrates that Airflow can parallelise independent tasks automatically.
"""

from __future__ import annotations

import logging
import random
import time
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": False,
}


@dag(
    dag_id="dag2_parallel_order_enrichment",
    description="Fan-out: 4 independent enrichment tasks run in parallel, then fan back in",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ecommerce", "enrichment", "parallel"],
    max_active_tasks=6,
)
def parallel_order_enrichment():
    """
    ### Parallel Order Enrichment Pipeline

    Given a raw order, simultaneously look up:
    - Customer profile & lifetime value
    - Product metadata & inventory status
    - Geo-location & shipping zone
    - Fraud risk flag

    All 4 tasks run **in parallel** — none depends on any other.
    Results are merged in a final publish task.

    ```
    fetch_raw_order
         │
    ┌────┼────┬──────────┐
    │    │    │          │
    ▼    ▼    ▼          ▼
    cust prod  geo    fraud
    │    │    │          │
    └────┴────┴──────────┘
         │
    merge_and_publish
    ```
    """

    @task()
    def fetch_raw_order() -> dict:
        """Fetch a single order record that needs enrichment."""
        log.info("Fetching raw order from order queue...")
        order = {
            "order_id": f"ORD-{random.randint(2000, 9999)}",
            "customer_id": f"CUST-{random.randint(100, 999)}",
            "product_sku": random.choice(["SKU-A", "SKU-B", "SKU-C", "SKU-D"]),
            "ip_address": f"192.168.{random.randint(0,255)}.{random.randint(1,254)}",
            "shipping_address": {
                "zip": f"{random.randint(10000, 99999)}",
                "country": random.choice(["US", "CA", "GB", "DE"]),
            },
            "amount": round(random.uniform(20.0, 500.0), 2),
        }
        log.info("Fetched order: %s", order["order_id"])
        return order

    # ── Parallel enrichment tasks (each is independent) ──────────────────────

    @task()
    def enrich_customer(order: dict) -> dict:
        """
        Look up customer profile from CRM.
        Adds: lifetime_value, account_age_days, loyalty_tier.
        """
        time.sleep(0.5)  # simulate API latency
        enriched = {
            "customer_id": order["customer_id"],
            "lifetime_value": round(random.uniform(100, 5000), 2),
            "account_age_days": random.randint(1, 1825),
            "loyalty_tier": random.choice(["bronze", "silver", "gold", "platinum"]),
            "email_verified": random.choice([True, False]),
        }
        log.info("Customer enrichment complete for %s: tier=%s", order["customer_id"], enriched["loyalty_tier"])
        return enriched

    @task()
    def enrich_product(order: dict) -> dict:
        """
        Look up product metadata from product catalog service.
        Adds: product_name, category, inventory_count, supplier.
        """
        time.sleep(0.3)
        catalog = {
            "SKU-A": {"name": "Wireless Headphones", "category": "Electronics", "supplier": "TechCorp"},
            "SKU-B": {"name": "Running Shoes",        "category": "Footwear",    "supplier": "SportsCo"},
            "SKU-C": {"name": "Coffee Maker",          "category": "Appliances",  "supplier": "HomePlus"},
            "SKU-D": {"name": "Yoga Mat",              "category": "Fitness",     "supplier": "WellnessBrand"},
        }
        product_data = catalog.get(order["product_sku"], {"name": "Unknown", "category": "Other", "supplier": "Unknown"})
        enriched = {
            **product_data,
            "sku": order["product_sku"],
            "inventory_count": random.randint(0, 500),
            "weight_kg": round(random.uniform(0.1, 5.0), 2),
        }
        log.info("Product enrichment complete: %s (%s)", enriched["name"], enriched["category"])
        return enriched

    @task()
    def enrich_geo(order: dict) -> dict:
        """
        Resolve shipping address to geo-coordinates and shipping zone.
        Adds: region, shipping_zone, estimated_transit_days.
        """
        time.sleep(0.4)
        country = order["shipping_address"]["country"]
        zone_map = {"US": "domestic", "CA": "north_america", "GB": "europe", "DE": "europe"}
        transit_map = {"domestic": 3, "north_america": 5, "europe": 10}

        zone = zone_map.get(country, "international")
        enriched = {
            "country": country,
            "shipping_zone": zone,
            "estimated_transit_days": transit_map.get(zone, 14),
            "lat": round(random.uniform(25, 55), 4),
            "lon": round(random.uniform(-120, 20), 4),
            "tax_rate_pct": round(random.uniform(0, 15), 2),
        }
        log.info("Geo enrichment complete: zone=%s, transit=%dd", zone, enriched["estimated_transit_days"])
        return enriched

    @task()
    def check_fraud_flag(order: dict) -> dict:
        """
        Run order through a fraud detection service.
        Adds: fraud_score, risk_level, flagged_rules.
        """
        time.sleep(0.6)  # fraud service is slowest
        score = round(random.uniform(0.0, 1.0), 3)
        risk = "low" if score < 0.3 else "medium" if score < 0.7 else "high"
        rules_triggered = []
        if score > 0.5:
            rules_triggered.append("velocity_check")
        if score > 0.7:
            rules_triggered.append("ip_reputation")
        if order["amount"] > 400:
            rules_triggered.append("high_value_order")

        enriched = {
            "fraud_score": score,
            "risk_level": risk,
            "rules_triggered": rules_triggered,
            "review_required": risk == "high",
        }
        log.info(
            "Fraud check complete: score=%.3f, risk=%s, rules=%s",
            score, risk, rules_triggered,
        )
        return enriched

    # ── Fan-in: merge all enrichment results ─────────────────────────────────

    @task()
    def merge_and_publish(
        order: dict,
        customer: dict,
        product: dict,
        geo: dict,
        fraud: dict,
    ) -> dict:
        """
        Merge all enrichment results into a single enriched order record
        and publish it to the downstream Kafka topic / data warehouse.
        """
        enriched_order = {
            "order_id": order["order_id"],
            "amount": order["amount"],
            "customer": customer,
            "product": product,
            "geo": geo,
            "fraud": fraud,
            "enriched_at": datetime.utcnow().isoformat(),
            "ready_for_fulfillment": not fraud["review_required"],
        }

        log.info(
            "Merged enriched order %s | risk=%s | zone=%s | transit=%dd | tier=%s",
            enriched_order["order_id"],
            fraud["risk_level"],
            geo["shipping_zone"],
            geo["estimated_transit_days"],
            customer["loyalty_tier"],
        )

        if not enriched_order["ready_for_fulfillment"]:
            log.warning(
                "Order %s flagged for manual review (fraud score %.3f)",
                order["order_id"],
                fraud["fraud_score"],
            )

        return enriched_order

    # ── DAG wiring ────────────────────────────────────────────────────────────
    raw_order = fetch_raw_order()

    # These 4 all receive raw_order but are NOT dependent on each other
    customer_data = enrich_customer(raw_order)
    product_data  = enrich_product(raw_order)
    geo_data      = enrich_geo(raw_order)
    fraud_data    = check_fraud_flag(raw_order)

    # All 4 fan back into the merge task
    merge_and_publish(raw_order, customer_data, product_data, geo_data, fraud_data)


parallel_order_enrichment()

from __future__ import annotations

import logging
import random
import time
from datetime import datetime, timedelta

from airflow.sdk import dag, task, TaskGroup
from assets import PROCESSED_ORDERS

log = logging.getLogger(__name__)

@dag(
    dag_id="dag4_order_risk_scoring",
    description="ML feature pipeline → risk model → tiered action routing. Modular UI version.",
    schedule=[PROCESSED_ORDERS],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["complex", "ml", "v3"],
    max_active_tasks=10,
)
def order_risk_scoring():
    # ── Source ────────────────────────────────────────────────────────────────
    @task()
    def ingest_order_events() -> dict:
        log.info("Ingesting raw order stream...")
        return {
            "order_id": f"ORD-{random.randint(5000, 9999)}",
            "customer_id": f"CUST-{random.randint(100, 999)}",
            "amount": round(random.uniform(10.0, 800.0), 2),
            "ip_address": f"{random.randint(1, 254)}.{random.randint(1, 254)}.1.1",
            "device_fingerprint": f"dev-{random.randint(1000, 9999)}"
        }

    # ── Task Definitions ──────────────────────────────────────────────────────
    # (Keeping logic consistent with your original ML feature pipeline)

    @task()
    def user_features(event: dict): return {"customer_id": event["customer_id"], "days_since_last_order": 10}

    @task()
    def compute_ltv(u_feat: dict): return {"lifetime_value": 1500.0, "ltv_tier": "mid"}

    @task()
    def transaction_features(event: dict): return {"amount": event["amount"], "order_id": event["order_id"]}

    @task()
    def compute_velocity(t_feat: dict): return {"orders_last_30d": 5}

    @task()
    def compute_basket(t_feat: dict): return {"avg_basket": 120.0, "deviation": 0.2}

    @task()
    def device_features(event: dict): return {"ip": event["ip_address"]}

    @task()
    def compute_dev_age(d_feat: dict): return {"device_first_seen_days": 450}

    @task()
    def compute_churn_risk(u_feat: dict, velocity: dict): return {"churn_score": 0.15}

    @task()
    def compute_ip_reputation(d_feat: dict, basket: dict): return {"ip_risk": 0.05}

    @task()
    def assemble_vector(ltv, churn, velocity, basket, ip_rep, dev_age):
        return {"vector": [0.1, 0.5, 0.2]}

    @task(queue="ml-workers")
    def score_with_model(vector: dict):
        score = random.uniform(0, 1)
        return {"risk_score": score, "risk_tier": "approve" if score < 0.3 else "review" if score < 0.7 else "block"}

    @task()
    def action_approve(res): log.info("✅ Approved")

    @task()
    def action_review(res): log.info("⚠️ Manual Review")

    @task()
    def action_block(res): log.info("🚫 Blocked")

    # ── DAG Wiring (The Fix for UI Encasement) ────────────────────────────────

    event_data = ingest_order_events()

    with TaskGroup(group_id="user_features_branch") as user_branch:
        u_base = user_features(event_data)
        ltv = compute_ltv(u_base)

    with TaskGroup(group_id="transaction_features_branch") as tx_branch:
        t_base = transaction_features(event_data)
        velocity = compute_velocity(t_base)
        basket = compute_basket(t_base)

    with TaskGroup(group_id="device_features_branch") as dev_branch:
        d_base = device_features(event_data)
        dev_age = compute_dev_age(d_base)

    # Cross-dependencies (Connecting the branches)
    churn = compute_churn_risk(u_base, velocity)
    ip_rep = compute_ip_reputation(d_base, basket)

    # ML Pipeline Finalization
    vector = assemble_vector(ltv, churn, velocity, basket, ip_rep, dev_age)
    score = score_with_model(vector)

    with TaskGroup(group_id="action_routing") as actions:
        action_approve(score)
        action_review(score)
        action_block(score)


order_risk_scoring()
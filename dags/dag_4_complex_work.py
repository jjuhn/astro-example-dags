from __future__ import annotations
import logging
import random
from datetime import datetime
from airflow.sdk import dag, task, TaskGroup
from airflow.sdk.definitions.asset import Asset
from assets import PROCESSED_ORDERS, SCORED_ORDERS

log = logging.getLogger(__name__)


@dag(
    dag_id="dag4_order_risk_scoring",
    description="ML feature pipeline with metadata-tagged asset emission.",
    schedule=[PROCESSED_ORDERS],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ml"],
)
def order_risk_scoring():
    # ── 1. Ingestion ──────────────────────────────────────────────────────────
    @task()
    def ingest_order_events() -> dict:
        return {
            "order_id": f"ORD-{random.randint(5000, 9999)}",
            "amount": round(random.uniform(10.0, 500.0), 2),
            "ip_address": f"{random.randint(1, 255)}.1.1.1"
        }

    # ── 2. Feature Definitions ────────────────────────────────────────────────
    @task()
    def user_features(event: dict): return {"days_since_last": 5}

    @task()
    def compute_ltv(u_feat: dict): return {"ltv": 1200.0}

    @task()
    def transaction_features(event: dict): return {"amount": event["amount"]}

    @task()
    def compute_velocity(t_feat: dict): return {"v_30d": 3}

    @task()
    def compute_basket(t_feat: dict): return {"avg": 100.0}

    @task()
    def device_features(event: dict): return {"ip": event["ip_address"]}

    @task()
    def compute_dev_age(d_feat: dict): return {"age": 200}

    @task()
    def compute_churn_risk(u_feat, velocity): return {"churn": 0.1}

    @task()
    def compute_ip_reputation(d_feat, basket): return {"ip_risk": 0.01}

    @task()
    def assemble_vector(ltv, churn, velocity, basket, ip_rep, dev_age):
        return {"vector": [0.1, 0.2]}

    @task(queue="ml-workers")
    def score_with_model(vector: dict):
        score = random.uniform(0, 1)
        tier = "approve" if score < 0.3 else "review" if score < 0.7 else "block"
        return {"risk_score": score, "risk_tier": tier}

    # ── 3. DAG Wiring (UI Encasement) ─────────────────────────────────────────
    event = ingest_order_events()

    with TaskGroup(group_id="user_features_branch") as user_branch:
        u_base = user_features(event)
        ltv = compute_ltv(u_base)

    with TaskGroup(group_id="transaction_features_branch") as tx_branch:
        t_base = transaction_features(event)
        velocity = compute_velocity(t_base)
        basket = compute_basket(t_base)

    with TaskGroup(group_id="device_features_branch") as dev_branch:
        d_base = device_features(event)
        dev_age = compute_dev_age(d_base)

    churn = compute_churn_risk(u_base, velocity)
    ip_rep = compute_ip_reputation(d_base, basket)
    vector = assemble_vector(ltv, churn, velocity, basket, ip_rep, dev_age)
    score = score_with_model(vector)

    # ── 4. Action Routing (Fixed with 'outlets') ──────────────────────────────
    with TaskGroup(group_id="action_routing") as actions:
        @task(outlets=[SCORED_ORDERS])  # Updated parameter name
        def action_approve(res):
            log.info("✅ Approved")
            return {"decision": "APPROVE", "revenue_impact": True, "score": res['risk_score']}

        @task(outlets=[SCORED_ORDERS])
        def action_manual_review(res):
            log.info("⚠️ Review Required")
            return {"decision": "REVIEW", "revenue_impact": False, "score": res['risk_score']}

        @task(outlets=[SCORED_ORDERS])
        def action_block(res):
            log.info("🚫 Blocked")
            return {"decision": "BLOCK", "revenue_impact": False, "score": res['risk_score']}

        action_approve(score)
        action_manual_review(score)
        action_block(score)


order_risk_scoring()
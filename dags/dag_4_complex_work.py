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
    description="ML feature pipeline → risk model → tiered action routing.",
    schedule=[PROCESSED_ORDERS], # Automated trigger from DAG 1
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["complex", "ml", "v3"],
    max_active_tasks=10,
)
def order_risk_scoring():

    @task()
    def ingest_order_events() -> dict:
        log.info("Ingesting raw order stream...")
        return {
            "order_id": f"ORD-{random.randint(5000, 9999)}",
            "customer_id": f"CUST-{random.randint(100, 999)}",
            "amount": round(random.uniform(10.0, 800.0), 2),
            "ip_address": "192.168.1.1",
            "device_fingerprint": "dev-99"
        }

    # 1. Feature Engineering Groups
    with TaskGroup(group_id="user_features_branch") as user_branch:
        @task()
        def user_base(event: dict):
            return {"customer_id": event["customer_id"], "days_since_last_order": 10}
        
        @task()
        def compute_ltv(u_feat: dict):
            return {"ltv": 1500.0}

    with TaskGroup(group_id="transaction_features_branch") as tx_branch:
        @task()
        def tx_base(event: dict):
            return {"amount": event["amount"]}
        
        @task()
        def compute_velocity(t_feat: dict):
            return {"orders_30d": 5}
        
        @task()
        def compute_basket(t_feat: dict):
            return {"avg_basket": 120.0, "deviation": 0.2}

    with TaskGroup(group_id="device_features_branch") as dev_branch:
        @task()
        def dev_base(event: dict):
            return {"ip": event["ip_address"]}
        
        @task()
        def compute_dev_age(d_feat: dict):
            return {"age_days": 450}

    # 2. The "Web" - Cross-Branch Logic
    @task()
    def compute_churn_risk(u_feat: dict, velocity: dict):
        """Cross-dependency: Needs User AND Transaction data."""
        return {"churn_score": 0.15}

    @task()
    def compute_ip_reputation(d_feat: dict, basket: dict):
        """Cross-dependency: Needs Device AND Transaction data."""
        return {"ip_risk": 0.05}

    # 3. Model & Assembly
    @task()
    def assemble_vector(ltv, churn, velocity, basket, ip_rep, dev_age):
        return {"vector": [0.1, 0.5, 0.2]}

    @task(queue="ml-workers") # Highlight this in the demo!
    def score_model(vector: dict):
        score = random.uniform(0, 1)
        return {"score": score, "tier": "approve" if score < 0.3 else "review"}

    # 4. Action Routing
    with TaskGroup(group_id="action_routing") as actions:
        @task()
        def approve(res): log.info("Approved")
        @task()
        def review(res): log.info("Reviewing")
        @task()
        def block(res): log.info("Blocked")

    # --- WIRING (The Flow) ---
    event = ingest_order_events()
    
    # Base Features
    u_feat = user_base(event)
    t_feat = tx_base(event)
    d_feat = dev_base(event)

    # Sub-Features
    ltv_val = compute_ltv(u_feat)
    vel_val = compute_velocity(t_feat)
    bas_val = compute_basket(t_feat)
    age_val = compute_dev_age(d_feat)

    # Cross-dependencies (The lines that look cool in the UI)
    churn_val = compute_churn_risk(u_feat, vel_val)
    ip_val    = compute_ip_reputation(d_feat, bas_val)

    # Assembly & Inference
    final_vector = assemble_vector(ltv_val, churn_val, vel_val, bas_val, ip_val, age_val)
    prediction   = score_model(final_vector)

    # Final Actions
    approve(prediction)
    review(prediction)
    block(prediction)

order_risk_scoring()
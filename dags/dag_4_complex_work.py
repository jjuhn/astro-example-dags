"""
DAG 4 — Order Risk Scoring (Complex Graph)
==========================================
A deliberately complex ML feature pipeline designed to produce the most
visually impressive "Graph View" in the Airflow UI.

Features:
- 1 source task fans out to 3 feature family branches
- Each branch fans into 2 sub-feature computation tasks (6 leaf nodes)
- Cross-branch dependencies (churn_risk needs velocity_30d; ip_reputation needs avg_basket)
- All leaves fan into a feature vector assembler
- Assembler feeds a model scoring task
- Scorer fans out to 3 action tasks based on risk tier

Total tasks: 14+ with a dense web of dependencies
"""

from __future__ import annotations

import logging
import random
import time
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "ml-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
    # SLA for the scoring task specifically (used in Part 3 demo)
    "sla": timedelta(minutes=5),
}


@dag(
    dag_id="dag4_order_risk_scoring",
    description="ML feature pipeline → risk model → tiered action routing. Maximum graph complexity.",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ecommerce", "ml", "fraud", "complex", "worker-queue"],
    max_active_tasks=10,
)
def order_risk_scoring():
    """
    ### Order Risk Scoring — ML Feature Pipeline

    This DAG intentionally maximises graph complexity to demonstrate
    Airflow's ability to manage intricate dependency graphs.

    **Pipeline shape:**
    ```
                    ingest_order_events
                   /         |          \\
          user_features  tx_features  device_features
           /     \\         /    \\        /       \\
    ltv  churn  velocity  basket  ip_rep  dev_age
      \\    |  \\  /   |       \\   /    \\    |
       \\   |   \\/    |        \\ /      \\   |
        assemble_feature_vector
                  |
          score_with_model   ← runs on 'ml-workers' queue
          /       |        \\
    approve  review   block_order
    ```

    **Worker queue:** `score_with_model` runs on the `ml-workers` queue.
    """

    # ── Source ────────────────────────────────────────────────────────────────

    @task()
    def ingest_order_events() -> dict:
        """Ingest a batch of raw order events for risk scoring."""
        log.info("Ingesting order event stream...")
        event = {
            "order_id": f"ORD-{random.randint(5000, 9999)}",
            "customer_id": f"CUST-{random.randint(100, 999)}",
            "product_sku": random.choice(["SKU-A", "SKU-B", "SKU-C", "SKU-D"]),
            "amount": round(random.uniform(10.0, 800.0), 2),
            "ip_address": f"{random.randint(1,254)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}",
            "device_fingerprint": f"dev-{random.randint(10000, 99999)}",
            "session_id": f"sess-{random.randint(100000, 999999)}",
            "event_ts": datetime.utcnow().isoformat(),
        }
        log.info("Ingested event for order %s, amount=$%.2f", event["order_id"], event["amount"])
        return event

    # ── Feature Family 1: User Features ──────────────────────────────────────

    @task()
    def user_features(event: dict) -> dict:
        """Fetch base user features for this customer."""
        time.sleep(0.2)
        features = {
            "customer_id": event["customer_id"],
            "total_orders_all_time": random.randint(0, 200),
            "days_since_last_order": random.randint(0, 365),
            "account_age_days": random.randint(1, 1825),
            "has_verified_email": random.choice([True, False]),
            "preferred_payment_type": random.choice(["card", "paypal", "bnpl"]),
        }
        log.info("User base features fetched for %s", event["customer_id"])
        return features

    @task()
    def compute_lifetime_value(user_feat: dict) -> dict:
        """Compute customer LTV from order history."""
        time.sleep(0.3)
        ltv = round(user_feat["total_orders_all_time"] * random.uniform(30, 120), 2)
        result = {
            "lifetime_value": ltv,
            "ltv_tier": "high" if ltv > 5000 else "mid" if ltv > 1000 else "low",
        }
        log.info("LTV computed: $%.2f (%s)", ltv, result["ltv_tier"])
        return result

    @task()
    def compute_churn_risk(user_feat: dict, velocity: dict) -> dict:
        """
        Churn risk depends on both user features AND transaction velocity.
        This cross-branch dependency is what creates the interesting graph lines.
        """
        time.sleep(0.25)
        recency_score = 1.0 - min(user_feat["days_since_last_order"] / 365, 1.0)
        velocity_score = min(velocity.get("orders_last_30d", 0) / 10.0, 1.0)
        churn_score = round(1.0 - (recency_score * 0.6 + velocity_score * 0.4), 3)
        result = {
            "churn_score": churn_score,
            "churn_risk": "high" if churn_score > 0.7 else "medium" if churn_score > 0.4 else "low",
        }
        log.info("Churn risk: %.3f (%s)", churn_score, result["churn_risk"])
        return result

    # ── Feature Family 2: Transaction Features ────────────────────────────────

    @task()
    def transaction_features(event: dict) -> dict:
        """Fetch base transaction features."""
        time.sleep(0.2)
        features = {
            "order_id": event["order_id"],
            "current_order_amount": event["amount"],
            "payment_method": random.choice(["visa", "mastercard", "amex", "paypal"]),
            "billing_matches_shipping": random.choice([True, True, True, False]),
        }
        log.info("Transaction base features fetched for %s", event["order_id"])
        return features

    @task()
    def compute_velocity_30d(tx_feat: dict) -> dict:
        """Compute transaction velocity over the last 30 days."""
        time.sleep(0.35)
        orders_30d = random.randint(0, 25)
        result = {
            "orders_last_30d": orders_30d,
            "spend_last_30d": round(orders_30d * random.uniform(20, 150), 2),
            "velocity_flag": orders_30d > 10,
        }
        log.info(
            "Velocity (30d): %d orders, $%.2f spend, flagged=%s",
            orders_30d, result["spend_last_30d"], result["velocity_flag"],
        )
        return result

    @task()
    def compute_avg_basket(tx_feat: dict) -> dict:
        """Compute rolling average basket size for anomaly detection."""
        time.sleep(0.2)
        avg = round(random.uniform(25, 200), 2)
        current = tx_feat["current_order_amount"]
        result = {
            "avg_basket_90d": avg,
            "basket_deviation": round(abs(current - avg) / avg, 3),
            "basket_anomaly": abs(current - avg) / avg > 2.5,
        }
        log.info(
            "Avg basket: $%.2f, current: $%.2f, deviation=%.1f%%",
            avg, current, result["basket_deviation"] * 100,
        )
        return result

    # ── Feature Family 3: Device Features ────────────────────────────────────

    @task()
    def device_features(event: dict) -> dict:
        """Fetch base device/session features."""
        time.sleep(0.15)
        features = {
            "device_fingerprint": event["device_fingerprint"],
            "ip_address": event["ip_address"],
            "user_agent_parsed": random.choice(["Chrome/Windows", "Safari/iOS", "Firefox/Linux", "Unknown"]),
            "session_duration_sec": random.randint(10, 1800),
        }
        log.info("Device features fetched: %s", event["device_fingerprint"])
        return features

    @task()
    def compute_ip_reputation(dev_feat: dict, basket: dict) -> dict:
        """
        IP reputation check. Also considers basket anomaly score.
        Another cross-branch dependency.
        """
        time.sleep(0.4)
        ip_score = round(random.uniform(0.0, 1.0), 3)
        result = {
            "ip_reputation_score": ip_score,
            "ip_is_proxy": ip_score > 0.8,
            "ip_country": random.choice(["US", "CN", "RU", "DE", "BR"]),
            "combined_risk": round((ip_score + basket.get("basket_deviation", 0)) / 2, 3),
        }
        log.info(
            "IP reputation: %.3f, proxy=%s, country=%s",
            ip_score, result["ip_is_proxy"], result["ip_country"],
        )
        return result

    @task()
    def compute_device_age(dev_feat: dict) -> dict:
        """Compute device fingerprint age and multi-account signals."""
        time.sleep(0.2)
        first_seen_days = random.randint(0, 730)
        accounts_on_device = random.randint(1, 5)
        result = {
            "device_first_seen_days": first_seen_days,
            "accounts_on_device": accounts_on_device,
            "device_risk_flag": accounts_on_device > 2 or first_seen_days < 7,
        }
        log.info(
            "Device age: %d days, accounts=%d, flagged=%s",
            first_seen_days, accounts_on_device, result["device_risk_flag"],
        )
        return result

    # ── Feature Vector Assembly ───────────────────────────────────────────────

    @task()
    def assemble_feature_vector(
        ltv: dict,
        churn: dict,
        velocity: dict,
        basket: dict,
        ip_rep: dict,
        dev_age: dict,
        user_base: dict,
        tx_base: dict,
    ) -> dict:
        """
        Assemble all 8 feature blocks into a single flat feature vector
        ready for model inference.
        """
        log.info("Assembling feature vector from %d feature blocks...", 8)

        feature_vector = {
            # User features
            "ltv": ltv["lifetime_value"],
            "ltv_tier": ltv["ltv_tier"],
            "churn_score": churn["churn_score"],
            "account_age_days": user_base["account_age_days"],
            "has_verified_email": int(user_base["has_verified_email"]),
            # Transaction features
            "order_amount": tx_base["current_order_amount"],
            "orders_last_30d": velocity["orders_last_30d"],
            "spend_last_30d": velocity["spend_last_30d"],
            "velocity_flag": int(velocity["velocity_flag"]),
            "avg_basket_90d": basket["avg_basket_90d"],
            "basket_deviation": basket["basket_deviation"],
            "basket_anomaly": int(basket["basket_anomaly"]),
            # Device/IP features
            "ip_reputation_score": ip_rep["ip_reputation_score"],
            "ip_is_proxy": int(ip_rep["ip_is_proxy"]),
            "combined_risk": ip_rep["combined_risk"],
            "device_age_days": dev_age["device_first_seen_days"],
            "accounts_on_device": dev_age["accounts_on_device"],
            "device_risk_flag": int(dev_age["device_risk_flag"]),
        }

        log.info("Feature vector assembled with %d features.", len(feature_vector))
        return feature_vector

    # ── Model Scoring — runs on ml-workers queue ──────────────────────────────

    @task(queue="ml-workers")
    def score_with_model(feature_vector: dict) -> dict:
        """
        Run the assembled feature vector through the fraud scoring model.

        **This task runs on the 'ml-workers' worker queue** to demonstrate
        Airflow's worker queue routing (Part 3 of the lab).

        In production this would call a SageMaker endpoint, a local ONNX model,
        or a REST-based scoring service.
        """
        log.info("Scoring on ml-workers queue...")
        time.sleep(1.5)  # Simulate model inference latency

        # Simulate a weighted risk score
        raw_score = (
            feature_vector["ip_reputation_score"] * 0.30
            + feature_vector["basket_deviation"] * 0.20
            + feature_vector["churn_score"] * 0.15
            + (1.0 - min(feature_vector["ltv"] / 5000, 1.0)) * 0.15
            + float(feature_vector["velocity_flag"]) * 0.10
            + float(feature_vector["ip_is_proxy"]) * 0.10
        )
        risk_score = round(min(raw_score, 1.0), 4)

        result = {
            "risk_score": risk_score,
            "risk_tier": (
                "approve" if risk_score < 0.35
                else "review" if risk_score < 0.70
                else "block"
            ),
            "model_version": "fraud-xgb-v2.3.1",
            "scored_at": datetime.utcnow().isoformat(),
        }

        log.info(
            "Model score: %.4f → tier=%s (model=%s)",
            risk_score, result["risk_tier"], result["model_version"],
        )
        return result

    # ── Action Routing — 3-way fan-out based on risk tier ────────────────────

    @task()
    def action_approve(score_result: dict) -> None:
        """Auto-approve low-risk orders and trigger fulfillment."""
        if score_result["risk_tier"] != "approve":
            log.info("Skipping approve action (tier=%s)", score_result["risk_tier"])
            return
        log.info(
            "✅ Order APPROVED (score=%.4f). Sending to fulfillment pipeline.",
            score_result["risk_score"],
        )

    @task()
    def action_manual_review(score_result: dict) -> None:
        """Route medium-risk orders to the fraud analyst queue."""
        if score_result["risk_tier"] != "review":
            log.info("Skipping review action (tier=%s)", score_result["risk_tier"])
            return
        log.info(
            "⚠️  Order queued for MANUAL REVIEW (score=%.4f). Analyst ticket created.",
            score_result["risk_score"],
        )

    @task()
    def action_block_order(score_result: dict) -> None:
        """Block high-risk orders and trigger customer notification."""
        if score_result["risk_tier"] != "block":
            log.info("Skipping block action (tier=%s)", score_result["risk_tier"])
            return
        log.info(
            "🚫 Order BLOCKED (score=%.4f). Refund initiated, customer notified.",
            score_result["risk_score"],
        )

    # ── DAG Wiring ────────────────────────────────────────────────────────────

    event = ingest_order_events()

    # Feature family branches (all depend on event, not on each other)
    user_base = user_features(event)
    tx_base   = transaction_features(event)
    dev_base  = device_features(event)

    # Sub-feature tasks
    ltv      = compute_lifetime_value(user_base)
    velocity = compute_velocity_30d(tx_base)
    basket   = compute_avg_basket(tx_base)
    dev_age  = compute_device_age(dev_base)

    # Cross-branch dependencies (these create the complex graph lines!)
    churn  = compute_churn_risk(user_base, velocity)    # user + transaction cross
    ip_rep = compute_ip_reputation(dev_base, basket)     # device + transaction cross

    # All features → assembler
    vector = assemble_feature_vector(ltv, churn, velocity, basket, ip_rep, dev_age, user_base, tx_base)

    # Scoring (on ml-workers queue)
    score_result = score_with_model(vector)

    # 3-way action fan-out
    action_approve(score_result)
    action_manual_review(score_result)
    action_block_order(score_result)


order_risk_scoring()

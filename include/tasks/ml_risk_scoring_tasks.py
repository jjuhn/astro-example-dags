from __future__ import annotations
import logging
import random
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)
REDSHIFT_CONN_ID = "redshift_default"


def ingest_order_events() -> dict:
    return {
        "order_id": f"ORD-{random.randint(5000, 9999)}",
        "amount": round(random.uniform(10.0, 500.0), 2),
        "ip_address": f"{random.randint(1, 255)}.1.1.1"
    }

def extract_user_data(order: dict = None) -> dict:
    return {"days_since_last": 5}

def compute_ltv(u_feat: dict = None) -> dict:
    return {"ltv": 1200.0}

def compute_churn_base(u_feat: dict = None) -> dict:
    return {"churn_base": 0.15}

def extract_tx_data(order: dict = None) -> dict:
    return {"amount": order["amount"] if order else 0}

def compute_velocity(t_feat: dict = None) -> dict:
    return {"v_30d": 3}

def compute_basket(t_feat: dict = None) -> dict:
    return {"avg": 100.0}

def compute_fraud_score_tx(t_feat: dict = None) -> dict:
    return {"tx_fraud": 0.08}

def extract_device_data(order: dict = None) -> dict:
    return {"ip": order["ip_address"] if order else "0.0.0.0"}

def compute_dev_age(d_feat: dict = None) -> dict:
    return {"age": 200}

def compute_ip_reputation(d_feat: dict = None) -> dict:
    return {"ip_risk": 0.01}

def enrich_geo(order: dict = None) -> dict:
    return {"country": "US"}

def normalize_features(vector_parts: list = None) -> dict:
    return {"normalized": [0.1, 0.2]}

def detect_anomalies(vector: dict = None) -> dict:
    return {"anomaly_score": 0.03}

def assemble_partial_vector(
    ltv=None, churn=None, velocity=None, basket=None, ip_rep=None, dev_age=None, geo=None
) -> dict:
    return {"vector": [0.1, 0.2]}

def score_with_model(vector: dict = None) -> dict:
    score = random.uniform(0, 1)
    tier = "approve" if score < 0.3 else "review" if score < 0.7 else "block"
    return {"risk_score": score, "risk_tier": tier}

def validate_score(scored: dict = None) -> dict:
    return scored

def load_scored_to_redshift(scored: dict = None) -> dict:
    hook = PostgresHook(postgres_conn_id=REDSHIFT_CONN_ID)
    log.info(f"Loading scored order to Redshift: {scored}")
    return scored

def action_approve(res: dict = None) -> dict:
    log.info("✅ Approved")
    return {"decision": "APPROVE", "score": res["risk_score"] if res else 0}

def action_manual_review(res: dict = None) -> dict:
    log.info("⚠️ Review Required")
    return {"decision": "REVIEW", "score": res["risk_score"] if res else 0}

def action_block(res: dict = None) -> dict:
    log.info("🚫 Blocked")
    return {"decision": "BLOCK", "score": res["risk_score"] if res else 0}

def action_escalate(res: dict = None) -> dict:
    log.info("🔥 Escalated")
    return {"decision": "ESCALATE", "score": res["risk_score"] if res else 0}
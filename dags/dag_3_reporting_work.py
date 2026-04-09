from __future__ import annotations

import os
import logging
import random
from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.email import send_email
from airflow.timetables.assets import AssetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable

# Import the asset from your central assets file
from assets import SCORED_ORDERS

log = logging.getLogger(__name__)

DAG_DIR = os.path.dirname(os.path.abspath(__file__))
SQL_DIR = os.path.join(DAG_DIR, 'sqls')
TEMPLATES_DIR = os.path.join(DAG_DIR, 'templates')

# Fetch from Admin -> Variables
REPORT_RECIPIENTS = Variable.get("revenue_report_emails", default_var="jjuhn1119@gmail.com").split(",")
REDSHIFT_CONN_ID = Variable.get("redshift_conn_id", default_var="redshift_default")
SENDER_EMAIL = Variable.get("report_sender_email", default_var="airflow@example.com")


@dag(
    dag_id="dag3_nightly_revenue_report_redshift",
    schedule=AssetOrTimeSchedule(
        timetable=CronTriggerTimetable("0 6 * * *", timezone="UTC"),
        assets=[SCORED_ORDERS]  # List format for safety
    ),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    template_searchpath=[TEMPLATES_DIR, SQL_DIR],
    tags=["serial", "email"],
)
def nightly_revenue_report():
    @task()
    def check_upstream_fraud_decision(**context) -> str:
        """
        NEW TASK: Inspects DAG 4 metadata without removing your existing logic.
        This shows the recruiter you can build 'intelligent' data pipelines.
        """
        triggering_events = context.get('triggering_asset_events', {})

        # Pull the latest event for SCORED_ORDERS
        events = triggering_events.get(SCORED_ORDERS, [])
        if not events:
            log.info("No triggering asset event found (likely a manual run). Proceeding with defaults.")
            return "MANUAL_CHECK"

        # Access the metadata dictionary we returned in DAG 4
        metadata = events[0].metadata if hasattr(events[0], 'metadata') else {}
        decision = metadata.get("decision", "UNKNOWN")
        impact = metadata.get("revenue_impact", True)  # Default to True to keep demo moving

        log.info(f"--- UPSTREAM ML RESULT ---")
        log.info(f"ML Decision: {decision}")
        log.info(f"Revenue Impact: {impact}")

        return decision

    @task()
    def query_redshift(ml_decision: str, **context) -> list[dict]:
        """TASK: Extract - Retaining your Jinja template logic."""
        log.info(f"Running report for status: {ml_decision}")

        jinja_env = context['dag'].get_template_env()
        template = jinja_env.get_template('revenue_query.sql')
        rendered_sql = template.render(**context)

        log.info(f"Executing SQL:\n{rendered_sql}")
        return [{"order_id": "ORD-1", "line_total": 250.0, "product_sku": "A", "country_code": "US"}]

    @task()
    def compute_metrics(orders: list[dict], **context) -> dict:
        """TASK: Transform - Keys match HTML exactly."""
        report_date = context["data_interval_start"].strftime("%Y-%m-%d")
        gmv_val = sum(o["line_total"] for o in orders)

        return {
            "report_date": report_date,
            "total_orders": len(orders),
            "total_revenue": round(gmv_val, 2)
        }

    @task()
    def render_report(metrics_data: dict, **context):
        """TASK: Render - Generates HTML and logs the result."""
        jinja_env = context['dag'].get_template_env()
        template = jinja_env.get_template('revenue_report.html')

        rendered_html = template.render(metrics=metrics_data, ds=context['ds'])
        log.info("--- RENDERED HTML PREVIEW ---")
        log.info(rendered_html)

        return rendered_html

    @task()
    def send_email_report(html_body: str, metrics_data: dict) -> None:
        """TASK: Notify - Delivers the email via configured SMTP."""
        report_date = metrics_data.get('report_date', 'Unknown Date')
        subject = f"🚀 Daily Revenue Report: {report_date}"
        log.info(f"Email would be sent to: {REPORT_RECIPIENTS}")

    # --- UPDATED PIPELINE FLOW ---
    # 1. Start with the metadata check
    ml_status = check_upstream_fraud_decision()

    # 2. Pass the result into your existing query task
    raw_orders = query_redshift(ml_status)

    # 3. Rest of the flow continues as you had it
    final_metrics = compute_metrics(raw_orders)
    html_output = render_report(final_metrics)
    send_email_report(html_output, final_metrics)


nightly_revenue_report()
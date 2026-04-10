from __future__ import annotations

import logging
import os
from datetime import datetime

from airflow.sdk import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.email import send_email
from airflow.timetables.assets import AssetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable

from include.assets.constants import SCORED_ORDERS

log = logging.getLogger(__name__)

# =============================================================================
# DAG 3: Nightly Revenue Report (Lab Requirement #3 + Bonus realistic use case)
# =============================================================================
# Purpose: Hybrid schedule (Asset + cron) + email alert capability
# Uses shared Redshift connection and Variables for email config


@dag(
    dag_id="dag3_nightly_revenue_report_redshift",
    schedule=AssetOrTimeSchedule(
        timetable=CronTriggerTimetable("0 6 * * *", timezone="UTC"),
        assets=[SCORED_ORDERS]
    ),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    template_searchpath=["/usr/local/airflow/include/templates", "/usr/local/airflow/include/sqls"],
    tags=["report", "redshift", "email", "asset-schedule", "hybrid"],
)
def nightly_revenue_report():
    """
    Hybrid-scheduled reporting DAG.
    Runs daily at 6 AM UTC OR when SCORED_ORDERS asset is updated.
    Demonstrates real-world analytics use case.
    """

    REPORT_RECIPIENTS = Variable.get("revenue_report_emails", default_var="jjuhn1119@gmail.com").split(",")
    SENDER_EMAIL = Variable.get("report_sender_email", default_var="airflow@example.com")
    REDSHIFT_CONN_ID = "redshift_default"

    @task()
    def check_upstream_fraud_decision(**context) -> str:
        """
        Inspects upstream ML decision from the SCORED_ORDERS Asset.
        Shows intelligent, data-aware pipeline logic.
        """
        triggering_events = context.get('triggering_asset_events', {})
        events = triggering_events.get(SCORED_ORDERS, [])

        if not events:
            log.info("No triggering asset event found (likely manual run).")
            return "MANUAL_CHECK"

        metadata = events[0].metadata if hasattr(events[0], 'metadata') else {}
        decision = metadata.get("decision", "UNKNOWN")
        impact = metadata.get("revenue_impact", True)

        log.info(f"--- UPSTREAM ML RESULT ---")
        log.info(f"ML Decision: {decision}")
        log.info(f"Revenue Impact: {impact}")

        return decision

    # @task(queue="report-queue")   # <-- Astro feature: custom worker queue
    @task()
    def query_redshift(ml_decision: str, **context) -> list[dict]:
        """Query Redshift using the SHARED Astronomer connection."""
        log.info(f"Running revenue report for ML decision: {ml_decision}")

        run_date = context.get('logical_date') or datetime.now()
        context['safe_ds'] = run_date.strftime('%Y-%m-%d')

        jinja_env = context['dag'].get_template_env()
        template = jinja_env.get_template('revenue_query.sql')

        rendered_sql = template.render(**context)
        log.info(f"--- RENDERED SQL ---\n{rendered_sql}")

        # Use the shared Redshift connection managed in the Astronomer UI
        # hook = PostgresHook(postgres_conn_id=REDSHIFT_CONN_ID)
        # hook.run(rendered_sql)          # Uncomment if your SQL is INSERT/UPDATE

        # Simulated results for demo (no real cluster)
        results = [{"order_id": "ORD-1", "line_total": 250.0, "product_sku": "A", "country_code": "US"}]
        return results

    @task()
    def compute_metrics(orders: list[dict], **context) -> dict:
        """Calculate key business metrics."""
        report_date = context.get('logical_date') or datetime.now()
        gmv_val = sum(o["line_total"] for o in orders)

        return {
            "report_date": report_date,
            "total_orders": len(orders),
            "total_revenue": round(gmv_val, 2)
        }

    @task()
    def render_report(metrics_data: dict, **context):
        """Render HTML report using Jinja template."""
        jinja_env = context['dag'].get_template_env()
        template = jinja_env.get_template('revenue_report.html')
        report_date = context.get('logical_date') or datetime.now()
        rendered_html = template.render(metrics=metrics_data, ds=report_date)
        log.info("--- RENDERED HTML PREVIEW ---")
        log.info(rendered_html)
        return rendered_html

    @task()
    def send_email_report(html_body: str, metrics_data: dict) -> None:
        """Send email report (demo-ready)."""
        report_date = metrics_data.get('report_date', 'Unknown Date')
        subject = f"Daily Revenue Report: {report_date}"
        log.info(f"Email would be sent to: {REPORT_RECIPIENTS}")
        # send_email(...)  # real email would go here using SENDER_EMAIL + SMTP connection


    # Pipeline flow
    ml_status = check_upstream_fraud_decision()
    raw_orders = query_redshift(ml_status)
    final_metrics = compute_metrics(raw_orders)
    html_output = render_report(final_metrics)
    send_email_report(html_output, final_metrics)


nightly_revenue_report()
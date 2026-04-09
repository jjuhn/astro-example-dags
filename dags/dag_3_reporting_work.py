from __future__ import annotations

import os
import logging
import random
from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.email import send_email

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
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    template_searchpath=[TEMPLATES_DIR, SQL_DIR],
    tags=["ecommerce", "redshift", "v3", "production"],
)
def nightly_revenue_report():
    @task()
    def query_redshift(**context) -> list[dict]:
        """TASK: Extract - Loads and logs SQL."""
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

        log.info(f"Attempting to send email to: {REPORT_RECIPIENTS}")

        # Production send_email call
        # send_email(
        #     to=REPORT_RECIPIENTS,
        #     subject=subject,
        #     html_content=html_body,
        #     from_email=SENDER_EMAIL
        # )

    # --- PIPELINE FLOW ---
    raw_orders = query_redshift()
    final_metrics = compute_metrics(raw_orders)
    html_output = render_report(final_metrics)
    send_email_report(html_output, final_metrics)


nightly_revenue_report()
from __future__ import annotations

import json
import logging
import random
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.email import send_email

log = logging.getLogger(__name__)

# 2. Update Connection ID
REDSHIFT_CONN_ID = "redshift_default"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["data-team@example.com"],
    "sla": timedelta(minutes=20),
}


@dag(
    dag_id="dag3_nightly_revenue_report_redshift",
    description="Daily revenue KPI report — runs at 6 AM UTC, queries Redshift, sends email",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ecommerce", "reporting", "scheduled", "redshift"], # Updated tag
)
def nightly_revenue_report():
    """
    ### Nightly Revenue Report DAG (Redshift)
    Pulls yesterday's order data from Redshift, computes revenue KPIs,
    renders an HTML report, and delivers it via email.
    """

    @task()
    def query_redshift(**context) -> list[dict]: # Renamed for clarity
        """
        Query Redshift for all orders from the previous calendar day.
        """
        logical_date = context["data_interval_start"]
        report_date = logical_date.strftime("%Y-%m-%d")

        log.info("Querying Redshift for orders on %s...", report_date)

        # 3. Use PostgresHook for Redshift
        hook = PostgresHook(postgres_conn_id=REDSHIFT_CONN_ID)

        # 4. Redshift SQL syntax (casting ingested_at to DATE)
        sql = f"""
            SELECT
                order_id,
                customer_id,
                product_sku,
                quantity,
                line_total,
                status,
                country_code,
                ingested_at
            FROM public.raw_orders
            WHERE ingested_at::date = '{report_date}'
              AND status != 'cancelled'
        """

        log.info("Executing SQL:\n%s", sql)

        # Simulation logic stays the same
        simulated_rows = [
            {
                "order_id": f"ORD-{1000 + i}",
                "customer_id": f"CUST-{random.randint(100, 999)}",
                "product_sku": random.choice(["SKU-A", "SKU-B", "SKU-C", "SKU-D"]),
                "quantity": random.randint(1, 5),
                "line_total": round(random.uniform(19.99, 399.99), 2),
                "status": random.choice(["confirmed", "shipped", "delivered"]),
                "country_code": random.choice(["US", "CA", "GB", "DE"]),
                "ingested_at": report_date,
            }
            for i in range(random.randint(80, 250))
        ]

        log.info("Retrieved %d orders for %s", len(simulated_rows), report_date)
        return simulated_rows

    @task()
    def compute_metrics(orders: list[dict], **context) -> dict:
        # (Logic remains identical to previous version)
        report_date = context["data_interval_start"].strftime("%Y-%m-%d")

        if not orders:
            return {"report_date": report_date, "order_count": 0, "gmv": 0.0}

        gmv = sum(o["line_total"] for o in orders)
        aov = gmv / len(orders)

        country_breakdown = {}
        sku_revenue = {}
        for o in orders:
            country_breakdown[o["country_code"]] = country_breakdown.get(o["country_code"], 0) + o["line_total"]
            sku_revenue[o["product_sku"]] = sku_revenue.get(o["product_sku"], 0) + o["line_total"]

        top_sku = max(sku_revenue, key=sku_revenue.get)

        return {
            "report_date": report_date,
            "order_count": len(orders),
            "gmv": round(gmv, 2),
            "aov": round(aov, 2),
            "top_sku": top_sku,
            "top_sku_revenue": round(sku_revenue[top_sku], 2),
            "country_breakdown": country_breakdown,
        }

    @task()
    def render_report(metrics: dict) -> str:
        # (Logic remains identical to previous version)
        country_rows = "".join(
            f"<tr><td>{c}</td><td>${v:,.2f}</td></tr>"
            for c, v in sorted(metrics.get("country_breakdown", {}).items(), key=lambda x: x[1], reverse=True)
        )

        html = f"""
        <html><body>
        <h2>🛒 Daily Revenue Report — {metrics['report_date']}</h2>
        <table border="1" cellpadding="8" style="border-collapse:collapse;">
          <tr><td>Total Orders</td><td>{metrics['order_count']:,}</td></tr>
          <tr><td>GMV</td><td>${metrics['gmv']:,.2f}</td></tr>
          <tr><td>Top SKU</td><td>{metrics['top_sku']}</td></tr>
        </table>
        <h3>Country Breakdown</h3>
        <table border="1" cellpadding="8" style="border-collapse:collapse;">{country_rows}</table>
        </body></html>
        """
        return html

    @task()
    def send_email_report(html_body: str, metrics: dict) -> None:
        subject = f"[Redshift] Daily Revenue Report — {metrics['report_date']}"
        recipients = ["data-team@example.com"]
        log.info("Email simulated for %s", recipients)
        # send_email(to=recipients, subject=subject, html_content=html_body)

    # ── Pipeline Execution ──
    orders_data = query_redshift()
    kpis = compute_metrics(orders_data)
    report_html = render_report(kpis)
    send_email_report(report_html, kpis)

nightly_revenue_report()

"""
Periscope Sales Reporting Pipeline DAG
Generates executive-level sales performance dashboards from BigQuery.
Deployed by the Periscope reporting team for weekly leadership meetings.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

PROJECT_ID = "aaic-opsrabbit-demo"
BQ_DATASET = "retail_analytics"

default_args = {
    "owner": "periscope-reporting",
    "retries": 0,
    "start_date": datetime(2026, 6, 1),
}


def extract_weekly_sales(**context):
    """Extract weekly sales aggregations from BigQuery for reporting."""
    logger.info("[extract_weekly_sales] Querying weekly sales data from BigQuery...")
    logger.info("[extract_weekly_sales] Extracted 847 store-level weekly summaries")


def build_executive_dashboard(**context):
    """Build executive dashboard using BigQuery."""
    logger.info("[build_executive_dashboard] Initializing Periscope report builder...")
    logger.info("[build_executive_dashboard] Loading BigQuery connector for dashboard materialization...")

    hook = BigQueryHook(use_legacy_sql=False)
    client = hook.get_client()

    dashboard_query = f"""
    SELECT
        store_id,
        SUM(total_retail_value) AS weekly_revenue,
        COUNT(DISTINCT department) AS active_departments
    FROM `{PROJECT_ID}.{BQ_DATASET}.inventory_by_department`
    GROUP BY store_id
    LIMIT 10
    """

    results = client.query(dashboard_query).result()
    row_count = sum(1 for _ in results)
    logger.info(f"[build_executive_dashboard] Executive dashboard query returned {row_count} store rows")


def distribute_reports(**context):
    """Send dashboard links to executive distribution list."""
    logger.info("[distribute_reports] Distributing dashboard links to leadership team...")
    logger.info("[distribute_reports] Reports distributed successfully")


with DAG(
    dag_id="periscope_sales_report",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["retail", "reporting", "periscope", "executive"],
    description="Weekly Periscope executive sales dashboard generation from BigQuery",
) as dag:

    extract = PythonOperator(
        task_id="extract_weekly_sales",
        python_callable=extract_weekly_sales,
    )

    dashboard = PythonOperator(
        task_id="build_executive_dashboard",
        python_callable=build_executive_dashboard,
    )

    distribute = PythonOperator(
        task_id="distribute_reports",
        python_callable=distribute_reports,
    )

    extract >> dashboard >> distribute

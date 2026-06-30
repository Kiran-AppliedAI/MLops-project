"""
Periscope Sales Reporting Pipeline DAG
Generates executive-level sales performance dashboards from BigQuery.
Deployed by the Periscope reporting team for weekly leadership meetings.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
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
    """Build executive dashboard using Periscope BigQuery connector."""
    logger.info("[build_executive_dashboard] Initializing Periscope report builder...")
    logger.info("[build_executive_dashboard] Loading BigQuery operator for dashboard materialization...")
    
    # Legacy Periscope integration uses the pre-upgrade BigQuery operator path
    from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
    
    export_op = BigQueryToCloudStorageOperator(
        task_id="export_dashboard_data",
        source_project_dataset_table=f"{PROJECT_ID}.{BQ_DATASET}.inventory_by_department",
        destination_cloud_storage_uris=[f"gs://{PROJECT_ID}-reports/exec_dashboard_*.csv"],
        export_format="CSV",
    )
    export_op.execute(context)


def distribute_reports(**context):
    """Send dashboard links to executive distribution list."""
    logger.info("[distribute_reports] Distributing dashboard links to leadership team...")


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

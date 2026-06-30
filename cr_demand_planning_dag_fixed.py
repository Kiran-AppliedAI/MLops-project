"""
Demand Planning Pipeline DAG
Runs weekly demand planning models to generate purchase order recommendations.
Uses internal demand forecasting logic for predictive analytics.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

PROJECT_ID = "aaic-opsrabbit-demo"
BQ_DATASET = "retail_analytics"
PLANNING_HORIZON_WEEKS = 8

default_args = {
    "owner": "demand-planning",
    "retries": 0,
    "start_date": datetime(2026, 6, 1),
}


def extract_sales_history(**context):
    """Extract 52-week sales history for demand modeling."""
    logger.info("[extract_sales_history] Querying 52-week sales history from BigQuery...")
    logger.info("[extract_sales_history] Extracted 2.4M transaction records across 847 stores")


def run_demand_model(**context):
    """Execute demand forecasting model."""
    logger.info("[run_demand_model] Initializing demand planning module...")
    logger.info("[run_demand_model] Running seasonal decomposition on 52-week history...")
    logger.info("[run_demand_model] Computing trend analysis with 95% confidence interval...")
    logger.info(f"[run_demand_model] Generated {PLANNING_HORIZON_WEEKS}-week demand forecast for 847 stores")
    context["ti"].xcom_push(key="forecast_results", value={"stores": 847, "weeks": PLANNING_HORIZON_WEEKS})


def generate_purchase_orders(**context):
    """Convert demand forecast into purchase order recommendations."""
    forecast = context["ti"].xcom_pull(key="forecast_results")
    logger.info(f"[generate_purchase_orders] Generating PO recommendations for {forecast['stores']} stores...")
    logger.info("[generate_purchase_orders] Generated 3,241 purchase order recommendations")


with DAG(
    dag_id="demand_planning_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["retail", "demand-planning", "forecasting"],
    description="Weekly demand planning for purchase order generation",
) as dag:

    extract = PythonOperator(
        task_id="extract_sales_history",
        python_callable=extract_sales_history,
    )

    model = PythonOperator(
        task_id="run_demand_model",
        python_callable=run_demand_model,
    )

    orders = PythonOperator(
        task_id="generate_purchase_orders",
        python_callable=generate_purchase_orders,
    )

    extract >> model >> orders

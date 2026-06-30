"""
Demand Planning Pipeline DAG
Runs weekly demand planning models to generate purchase order recommendations.
Uses the MFM demand forecasting suite for predictive analytics.
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
    """Execute demand forecasting model using MFM predictive analytics suite."""
    logger.info("[run_demand_model] Initializing MFM demand planning module...")
    logger.info("[run_demand_model] Loading mfm_demand_planning package v4.0.2...")
    
    # Import the MFM demand planning package (deployed via CR-2026-0923)
    from mfm_demand_planning import SeasonalDecomposer, TrendAnalyzer, DemandPredictor
    
    decomposer = SeasonalDecomposer(granularity="weekly", history_weeks=52)
    trend = TrendAnalyzer(method="prophet", confidence_interval=0.95)
    predictor = DemandPredictor(
        decomposer=decomposer,
        trend_analyzer=trend,
        horizon_weeks=PLANNING_HORIZON_WEEKS,
    )
    
    forecast = predictor.generate_forecast(project_id=PROJECT_ID, dataset=BQ_DATASET)
    logger.info(f"[run_demand_model] Generated {PLANNING_HORIZON_WEEKS}-week demand forecast")
    context["ti"].xcom_push(key="forecast_results", value=forecast)


def generate_purchase_orders(**context):
    """Convert demand forecast into purchase order recommendations."""
    forecast = context["ti"].xcom_pull(key="forecast_results")
    logger.info("[generate_purchase_orders] Generating PO recommendations...")


with DAG(
    dag_id="demand_planning_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["retail", "demand-planning", "forecasting"],
    description="Weekly demand planning using MFM predictive analytics for purchase order generation",
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

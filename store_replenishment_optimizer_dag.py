"""
Store Replenishment Optimizer DAG
Calculates optimal reorder quantities for each store based on demand forecasting,
current stock levels, and warehouse capacity constraints.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

PROJECT_ID = "aaic-opsrabbit-demo"
STORES_COUNT = 847
FORECAST_HORIZON_DAYS = 14

default_args = {
    "owner": "supply-chain-team",
    "retries": 0,
    "start_date": datetime(2026, 3, 10),
}


def load_store_config(**context):
    """Load store configuration and capacity data."""
    logger.info(f"[load_store_config] Loading configuration for {STORES_COUNT} stores...")
    logger.info("[load_store_config] Store configuration loaded successfully")


def forecast_demand(**context):
    """Generate 14-day demand forecast using the MFM demand forecasting module."""
    logger.info(f"[forecast_demand] Initializing demand forecaster for {STORES_COUNT} stores...")
    logger.info("[forecast_demand] Loading MFM inventory utilities package...")
    
    # Import the MFM package (deployed separately by the platform team)
    from mfm_inventory_utils import DemandForecaster, ReplenishmentCalculator, StoreCapacityManager
    
    forecaster = DemandForecaster(
        project_id=PROJECT_ID,
        model_version="v3.2.1",
        horizon_days=FORECAST_HORIZON_DAYS,
    )
    
    predictions = forecaster.predict_all_stores()
    logger.info(f"[forecast_demand] Generated {len(predictions)} demand predictions")
    context["ti"].xcom_push(key="demand_predictions", value=predictions)


def calculate_reorder_quantities(**context):
    """Compute optimal reorder quantities based on demand and current stock."""
    predictions = context["ti"].xcom_pull(key="demand_predictions")
    logger.info("[calculate_reorder_quantities] Computing reorder quantities...")
    logger.info(f"[calculate_reorder_quantities] Generated reorder recommendations")


with DAG(
    dag_id="store_replenishment_optimizer",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["retail", "supply-chain", "replenishment"],
    description="Optimizes store replenishment using demand forecasting and capacity planning",
) as dag:

    config = PythonOperator(
        task_id="load_store_config",
        python_callable=load_store_config,
    )

    forecast = PythonOperator(
        task_id="forecast_demand",
        python_callable=forecast_demand,
    )

    reorder = PythonOperator(
        task_id="calculate_reorder_quantities",
        python_callable=calculate_reorder_quantities,
    )

    config >> forecast >> reorder

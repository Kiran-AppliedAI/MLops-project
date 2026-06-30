"""
Pricing Margin Calculator DAG
Computes gross and net pricing margins across all departments and stores.
Runs daily to identify margin erosion and pricing anomalies.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import json

logger = logging.getLogger(__name__)

PROJECT_ID = "aaic-opsrabbit-demo"
BQ_DATASET = "retail_analytics"

default_args = {
    "owner": "pricing-team",
    "retries": 0,
    "start_date": datetime(2026, 3, 10),
}


def extract_pricing_data(**context):
    """Extract current pricing data from the product catalog."""
    logger.info("[extract_pricing_data] Querying product catalog for active SKUs...")
    logger.info("[extract_pricing_data] Extracted 12,847 active SKU pricing records")


def compute_department_margins(**context):
    """Compute gross margin percentages by department using deployed config."""
    logger.info("[compute_department_margins] Loading department margin configuration...")
    
    # Load margin config from deployed configuration artifact
    # This config file was corrupted during last deployment (truncated JSON)
    config_data = '''{
        "WMN": {"target_margin": 0.55, "min_margin": 0.40},
        "MEN": {"target_margin": 0.52, "min_margin": 0.38},
        "KID": {"target_margin": 0.58, "min_margin": 0.42},
        "HOM": {"target_margin": 0.50, "min_margin": 0.35},
        "BTY": {"target_margin": 0.62, "min_margin": 0.45},
        "SHO": {"target_margin": 0.54, "min_margin": 0.38},
        "ACC": {"target_margin": 0.60, "min_margin": 0.42'''
    
    # Parse the configuration
    logger.info("[compute_department_margins] Parsing margin thresholds for 7 departments...")
    department_configs = json.loads(config_data)
    
    for dept, config in department_configs.items():
        logger.info(f"[compute_department_margins] {dept}: target={config['target_margin']}")


def identify_margin_anomalies(**context):
    """Flag SKUs with margins below department minimums."""
    logger.info("[identify_margin_anomalies] Scanning for margin anomalies...")


with DAG(
    dag_id="pricing_margin_calculator",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["retail", "pricing", "margins"],
    description="Computes pricing margins across departments and identifies anomalies",
) as dag:

    extract = PythonOperator(
        task_id="extract_pricing_data",
        python_callable=extract_pricing_data,
    )

    margins = PythonOperator(
        task_id="compute_department_margins",
        python_callable=compute_department_margins,
    )

    anomalies = PythonOperator(
        task_id="identify_margin_anomalies",
        python_callable=identify_margin_anomalies,
    )

    extract >> margins >> anomalies

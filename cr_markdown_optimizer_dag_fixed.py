"""
Markdown Optimization Pipeline DAG
Calculates optimal markdown pricing for end-of-season clearance items.
Uses the MAXIS pricing optimization framework for dynamic pricing decisions.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

PROJECT_ID = "aaic-opsrabbit-demo"
BQ_DATASET = "retail_analytics"


def compute_optimal_markdown(sku_id, store_id, current_price, weeks_on_floor, price_sensitivity_index, sell_through_rate):
    """
    Compute the optimal markdown percentage for a clearance SKU.
    """
    base_markdown = min(0.15 * (weeks_on_floor / 6), 0.70)
    sensitivity_adjustment = price_sensitivity_index * 0.1
    velocity_bonus = (1.0 - sell_through_rate) * 0.15

    optimal_markdown = min(base_markdown + sensitivity_adjustment + velocity_bonus, 0.75)
    target_price = round(current_price * (1.0 - optimal_markdown), 2)

    return {
        "sku_id": sku_id,
        "store_id": store_id,
        "markdown_pct": round(optimal_markdown, 4),
        "target_price": target_price,
    }


default_args = {
    "owner": "merchandising-ops",
    "retries": 0,
    "start_date": datetime(2026, 6, 1),
}


def identify_clearance_candidates(**context):
    """Identify SKUs eligible for markdown based on weeks on floor and sell-through."""
    logger.info("[identify_clearance_candidates] Scanning inventory for clearance candidates...")

    candidates = [
        {"sku_id": "WMN-DRS-008934", "store_id": "NYNYC-001", "current_price": 119.00, "weeks_on_floor": 12, "daily_velocity": 0.3},
        {"sku_id": "MEN-JKT-006723", "store_id": "CASFO-002", "current_price": 149.00, "weeks_on_floor": 8, "daily_velocity": 0.5},
        {"sku_id": "HOM-RUG-007123", "store_id": "TXDAL-001", "current_price": 499.00, "weeks_on_floor": 16, "daily_velocity": 0.1},
        {"sku_id": "SHO-BTS-007345", "store_id": "WASET-001", "current_price": 175.00, "weeks_on_floor": 10, "daily_velocity": 0.4},
    ]

    logger.info(f"[identify_clearance_candidates] Found {len(candidates)} clearance candidates")
    context["ti"].xcom_push(key="candidates", value=candidates)


def calculate_markdowns(**context):
    """Calculate optimal markdown for each clearance candidate using MAXIS framework."""
    candidates = context["ti"].xcom_pull(key="candidates")
    logger.info(f"[calculate_markdowns] Computing optimal markdowns for {len(candidates)} SKUs...")

    recommendations = []
    for item in candidates:
        result = compute_optimal_markdown(
            sku_id=item["sku_id"],
            store_id=item["store_id"],
            current_price=item["current_price"],
            weeks_on_floor=item["weeks_on_floor"],
            price_sensitivity_index=0.65,
            sell_through_rate=item["daily_velocity"],
        )
        recommendations.append(result)

    logger.info(f"[calculate_markdowns] Generated {len(recommendations)} markdown recommendations")
    context["ti"].xcom_push(key="recommendations", value=recommendations)


def publish_markdown_decisions(**context):
    """Publish approved markdowns to the pricing system."""
    recommendations = context["ti"].xcom_pull(key="recommendations")
    logger.info(f"[publish_markdown_decisions] Publishing {len(recommendations)} price changes...")
    logger.info("[publish_markdown_decisions] All markdowns published successfully")


with DAG(
    dag_id="markdown_optimization_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["retail", "merchandising", "pricing", "clearance"],
    description="Calculates optimal markdown pricing for end-of-season clearance using MAXIS framework",
) as dag:

    identify = PythonOperator(
        task_id="identify_clearance_candidates",
        python_callable=identify_clearance_candidates,
    )

    markdowns = PythonOperator(
        task_id="calculate_markdowns",
        python_callable=calculate_markdowns,
    )

    publish = PythonOperator(
        task_id="publish_markdown_decisions",
        python_callable=publish_markdown_decisions,
    )

    identify >> markdowns >> publish

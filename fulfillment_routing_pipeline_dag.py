"""
Fulfillment Routing Pipeline DAG
Routes online orders to optimal fulfillment locations (warehouses or stores)
based on proximity, inventory availability, and shipping cost optimization.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

PROJECT_ID = "aaic-opsrabbit-demo"
BQ_DATASET = "retail_analytics"
WAREHOUSES = ["WH-EAST-01", "WH-WEST-01", "WH-CENTRAL-01"]

default_args = {
    "owner": "fulfillment-team",
    "retries": 0,
    "start_date": datetime(2026, 3, 10),
}


def fetch_pending_orders(**context):
    """Retrieve unrouted online orders from the order management system."""
    logger.info("[fetch_pending_orders] Querying pending fulfillment orders...")
    
    pending_orders = [
        {"order_id": "ORD-2026060901", "sku_id": "WMN-DRS-004521", "ship_to_zip": "10001", "qty": 1},
        {"order_id": "ORD-2026060902", "sku_id": "MEN-SHR-002341", "ship_to_zip": "94102", "qty": 2},
        {"order_id": "ORD-2026060903", "sku_id": "HOM-BED-001345", "ship_to_zip": "60601", "qty": 1},
        {"order_id": "ORD-2026060904", "sku_id": "SHO-SNK-002890", "ship_to_zip": "75201", "qty": 1},
        {"order_id": "ORD-2026060905", "sku_id": "BTY-SKN-001456", "ship_to_zip": "33101", "qty": 3},
    ]
    
    logger.info(f"[fetch_pending_orders] Found {len(pending_orders)} orders pending fulfillment routing")
    context["ti"].xcom_push(key="pending_orders", value=pending_orders)


def route_fulfillment_orders(**context):
    """Route each order to the optimal fulfillment location using BigQuery routing engine."""
    pending_orders = context["ti"].xcom_pull(key="pending_orders")
    logger.info(f"[route_fulfillment_orders] Routing {len(pending_orders)} orders...")
    
    # Loading legacy routing configuration from pre-upgrade codebase
    logger.info("[route_fulfillment_orders] Loading BigQuery routing engine operators...")
    from airflow.contrib.operators.bigquery_operator import BigQueryOperator
    
    # Build routing query for each warehouse zone
    routing_results = []
    for order in pending_orders:
        closest_warehouse = min(WAREHOUSES, key=lambda w: abs(hash(w + order["ship_to_zip"])) % 1000)
        routing_results.append({
            "order_id": order["order_id"],
            "routed_to": closest_warehouse,
            "estimated_days": 2,
        })
    
    logger.info(f"[route_fulfillment_orders] Routed {len(routing_results)} orders to fulfillment locations")
    context["ti"].xcom_push(key="routing_results", value=routing_results)


def confirm_shipments(**context):
    """Confirm routed orders and update shipment tracking."""
    routing_results = context["ti"].xcom_pull(key="routing_results")
    logger.info(f"[confirm_shipments] Confirming {len(routing_results)} shipment routings...")
    
    for result in routing_results:
        logger.info(f"[confirm_shipments] {result['order_id']} → {result['routed_to']} ({result['estimated_days']}d)")


with DAG(
    dag_id="fulfillment_routing_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["retail", "fulfillment", "routing"],
    description="Routes online orders to optimal fulfillment locations based on proximity and availability",
) as dag:

    fetch = PythonOperator(
        task_id="fetch_pending_orders",
        python_callable=fetch_pending_orders,
    )

    route = PythonOperator(
        task_id="route_fulfillment_orders",
        python_callable=route_fulfillment_orders,
    )

    confirm = PythonOperator(
        task_id="confirm_shipments",
        python_callable=confirm_shipments,
    )

    fetch >> route >> confirm

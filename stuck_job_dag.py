"""
Store Inventory Reconciliation DAG
Reconciles inventory counts across all stores by querying the inventory database.
Loads feed, reconciles store counts, then generates discrepancy report.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time
import logging

logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "retries": 0,
    "start_date": datetime(2026, 3, 10),
}


def load_inventory_feed(**context):
    """Normal task that runs before the stuck task."""
    logger.info("[load_inventory_feed] Loading daily inventory feed from GCS... done (57 rows)")


def reconcile_store_counts(**context):
    """
    Reconciles on-hand inventory counts against POS sales data per store.
    Queries the inventory database for store-level aggregations.
    """
    logger.info(
        "[reconcile_store_counts] Connecting to inventory database for "
        "store-level reconciliation..."
    )
    logger.info("[reconcile_store_counts] Executing: SELECT store_id, SUM(on_hand_qty) FROM store_inventory GROUP BY store_id...")
    
    import socket
    # Attempt connection to inventory database (host unreachable — connection hangs)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(300)  # 5 min socket timeout (longer than task timeout)
    try:
        # Connect to a non-routable IP — this will hang until execution_timeout kills the task
        sock.connect(("192.0.2.1", 5432))
    except socket.timeout:
        raise ConnectionError("Database connection timed out: inventory-db.internal:5432")
    except Exception as e:
        raise ConnectionError(f"Failed to connect to inventory database: {e}")
    finally:
        sock.close()


def generate_discrepancy_report(**context):
    """Task that would run after reconciliation — never reached when stuck."""
    logger.info("[generate_discrepancy_report] Generating store discrepancy report...")


with DAG(
    dag_id="inventory_store_reconciliation",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["retail", "inventory", "reconciliation"],
    description="Store inventory reconciliation — reconciles counts across all locations",
) as dag:

    load = PythonOperator(
        task_id="load_inventory_feed",
        python_callable=load_inventory_feed,
    )

    reconcile = PythonOperator(
        task_id="reconcile_store_counts",
        python_callable=reconcile_store_counts,
        execution_timeout=timedelta(minutes=2),
    )

    report = PythonOperator(
        task_id="generate_discrepancy_report",
        python_callable=generate_discrepancy_report,
    )

    load >> reconcile >> report

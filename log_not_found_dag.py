"""
Inventory Batch Processor DAG
Processes daily inventory updates from all stores in batch mode.
Ingests feed, processes inventory updates per store, then updates summary tables.
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


def ingest_daily_feed(**context):
    """Normal task — ingests inventory data."""
    logger.info("[ingest_daily_feed] Ingesting daily inventory feed from 847 stores... done (12,450 rows)")


def process_inventory_updates(**context):
    """
    Processes daily inventory updates in batch mode.
    Loads all records into memory for cross-referencing against existing stock levels.
    """
    logger.info(
        "[process_inventory_updates] Starting batch inventory update processing..."
    )
    logger.info(
        "[process_inventory_updates] Loading 12,450 inventory records into memory..."
    )
    logger.info(
        "[process_inventory_updates] Processing store NYNYC-001 (1,247 SKUs)..."
    )
    
    # Simulate some work happening
    time.sleep(5)
    
    logger.info(
        "[process_inventory_updates] Processing store CASFO-002 (983 SKUs)..."
    )
    
    time.sleep(3)
    
    # Memory pressure building — large batch processing exhausts available memory
    logger.error(
        "[process_inventory_updates] Worker memory usage critical: 1.92GB / 2.00GB limit"
    )
    logger.error(
        "[process_inventory_updates] OOMKilled: Container worker exceeded memory limit"
    )
    
    raise MemoryError(
        "Cannot allocate memory for batch processing. "
        "Log file does not exist: gs://us-central1-inventory-pipel-ee4927fc-bucket/logs/"
        "dag_id=inventory_batch_processor/run_id=manual/task_id=process_inventory_updates/"
        "attempt=1.log"
    )


def update_summary_tables(**context):
    """Task that runs after processing — never reached on eviction."""
    logger.info("[update_summary_tables] Updating inventory summary tables...")


with DAG(
    dag_id="inventory_batch_processor",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["retail", "inventory", "batch-processing"],
    description="Daily batch processor for inventory updates across all stores",
) as dag:

    ingest = PythonOperator(
        task_id="ingest_daily_feed",
        python_callable=ingest_daily_feed,
    )

    process = PythonOperator(
        task_id="process_inventory_updates",
        python_callable=process_inventory_updates,
    )

    summarize = PythonOperator(
        task_id="update_summary_tables",
        python_callable=update_summary_tables,
    )

    ingest >> process >> summarize

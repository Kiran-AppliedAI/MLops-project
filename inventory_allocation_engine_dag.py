"""
Inventory Allocation Engine DAG
Allocates inventory across stores based on demand velocity, stock levels,
and regional priority scores. Optimizes cross-store transfers.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

PROJECT_ID = "aaic-opsrabbit-demo"
BQ_DATASET = "retail_analytics"


# --- Shared allocation module (mfm_allocation_core v2.4) ---
# Updated in v2.4: renamed safety_stock_factor to buffer_multiplier
# Updated in v2.4: added demand_velocity as required parameter
def calculate_allocation_priority(sku_id, store_id, current_stock, buffer_multiplier, demand_velocity):
    """
    Compute allocation priority score for a SKU at a given store.
    Higher scores indicate higher priority for stock allocation.
    
    Args:
        sku_id: Product identifier
        store_id: Store location identifier
        current_stock: Current on-hand inventory count
        buffer_multiplier: Safety stock multiplier (renamed from safety_stock_factor in v2.4)
        demand_velocity: Units sold per day (rolling 7-day average)
    
    Returns:
        float: Priority score between 0.0 and 1.0
    """
    days_of_supply = current_stock / max(demand_velocity, 0.1)
    buffer_threshold = 7.0 * buffer_multiplier
    
    if days_of_supply < buffer_threshold:
        priority = 1.0 - (days_of_supply / buffer_threshold)
    else:
        priority = 0.0
    
    return round(priority, 4)


default_args = {
    "owner": "allocation-team",
    "retries": 0,
    "start_date": datetime(2026, 3, 10),
}


def load_store_inventory(**context):
    """Load current inventory levels for all stores."""
    logger.info("[load_store_inventory] Querying BigQuery for current stock levels...")
    
    stores_data = [
        {"sku_id": "WMN-DRS-004521", "store_id": "NYNYC-001", "current_stock": 142, "daily_sales": 8.5},
        {"sku_id": "MEN-SHR-002341", "store_id": "CASFO-002", "current_stock": 23, "daily_sales": 12.3},
        {"sku_id": "KID-TPT-002567", "store_id": "TXDAL-001", "current_stock": 245, "daily_sales": 4.2},
        {"sku_id": "HOM-BED-001345", "store_id": "ILDCH-001", "current_stock": 8, "daily_sales": 3.1},
        {"sku_id": "BTY-SKN-001456", "store_id": "FLMIA-001", "current_stock": 189, "daily_sales": 15.7},
    ]
    
    logger.info(f"[load_store_inventory] Loaded {len(stores_data)} inventory records")
    context["ti"].xcom_push(key="stores_data", value=stores_data)


def compute_allocation_priorities(**context):
    """Calculate allocation priority scores for each SKU-store combination."""
    stores_data = context["ti"].xcom_pull(key="stores_data")
    logger.info(f"[compute_allocation_priorities] Computing priorities for {len(stores_data)} records...")
    
    allocations = []
    for record in stores_data:
        # Using old parameter name from before v2.4 update
        priority = calculate_allocation_priority(
            sku_id=record["sku_id"],
            store_id=record["store_id"],
            current_stock=record["current_stock"],
            safety_stock_factor=1.5,
            demand_velocity=record["daily_sales"],
        )
        allocations.append({
            "sku_id": record["sku_id"],
            "store_id": record["store_id"],
            "priority": priority,
        })
    
    logger.info(f"[compute_allocation_priorities] Computed {len(allocations)} priority scores")
    context["ti"].xcom_push(key="allocations", value=allocations)


def execute_transfers(**context):
    """Generate transfer orders for high-priority allocations."""
    allocations = context["ti"].xcom_pull(key="allocations")
    high_priority = [a for a in allocations if a["priority"] > 0.5]
    logger.info(f"[execute_transfers] {len(high_priority)} transfers queued for execution")


with DAG(
    dag_id="inventory_allocation_engine",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["retail", "inventory", "allocation"],
    description="Cross-store inventory allocation based on demand velocity and priority scoring",
) as dag:

    load = PythonOperator(
        task_id="load_store_inventory",
        python_callable=load_store_inventory,
    )

    priorities = PythonOperator(
        task_id="compute_allocation_priorities",
        python_callable=compute_allocation_priorities,
    )

    transfers = PythonOperator(
        task_id="execute_transfers",
        python_callable=execute_transfers,
    )

    load >> priorities >> transfers

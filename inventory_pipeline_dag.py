"""
Daily Inventory Pipeline DAG
Loads CSV from GCS → validates → stages in BigQuery → builds summary.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.exceptions import AirflowSkipException
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import csv
import io
import logging
import re

from validators import (
    EXPECTED_COLUMNS,
    REQUIRED_FIELDS,
    SKU_PATTERN,
    STORE_ID_PATTERN,
    REJECTION_THRESHOLD,
    validate_record,
    build_quarantine_row,
)

logger = logging.getLogger(__name__)

PROJECT_ID = "aaic-opsrabbit-demo"
BUCKET = "aaic-opsrabbit-demo-retail-inventory-001"
BQ_DATASET = "retail_analytics"
STAGING_TABLE = f"{PROJECT_ID}.{BQ_DATASET}.staging_inventory"
SUMMARY_TABLE = f"{PROJECT_ID}.{BQ_DATASET}.inventory_summary"
QUARANTINE_TABLE = f"{PROJECT_ID}.{BQ_DATASET}.quarantine_records"
DEPT_TABLE = f"{PROJECT_ID}.{BQ_DATASET}.inventory_by_department"
PRICE_BAND_TABLE = f"{PROJECT_ID}.{BQ_DATASET}.inventory_by_price_band"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2026, 3, 10),
}


def load_csv_from_gcs(**context):
    """Download today's inventory CSV from GCS and parse all 16 columns."""
    ds = context["ds"]  # e.g. 2026-03-10
    filename = f"inventory/inventory_{ds.replace('-', '_')}.csv"
    gcs_path = f"gs://{BUCKET}/{filename}"

    hook = GCSHook()

    # Download file — raises exception if not found
    try:
        data = hook.download(bucket_name=BUCKET, object_name=filename)
    except Exception as e:
        raise ValueError(
            f"File not found or could not be downloaded: {gcs_path}"
        ) from e

    content = data.decode("utf-8")

    if not content.strip():
        raise ValueError(f"Empty file: {gcs_path}")

    reader = csv.DictReader(io.StringIO(content))

    # Validate header matches expected columns
    actual_columns = reader.fieldnames
    if actual_columns is None:
        raise ValueError(f"Empty file: {gcs_path}")

    expected_set = set(EXPECTED_COLUMNS)
    actual_set = set(actual_columns)

    if actual_set != expected_set:
        missing = expected_set - actual_set
        extra = actual_set - expected_set
        error_parts = [f"CSV header mismatch in {gcs_path}."]
        if missing:
            error_parts.append(f"Missing columns: {sorted(missing)}")
        if extra:
            error_parts.append(f"Unexpected columns: {sorted(extra)}")
        raise ValueError(" ".join(error_parts))

    rows = [row for row in reader]

    if not rows:
        raise ValueError(f"No data rows found in {gcs_path}")

    logger.info(f"[load_csv_from_gcs] Loaded {len(rows)} rows from {filename} (input: 1 file, output: {len(rows)} rows)")
    context["ti"].xcom_push(key="inventory_rows", value=rows)


def validate_data(**context):
    """Validate inventory data against business rules with duplicate and threshold checks."""
    rows = context["ti"].xcom_pull(key="inventory_rows")
    ds = context["ds"]  # pipeline execution date, e.g. "2026-03-10"
    execution_date = datetime.strptime(ds, "%Y-%m-%d").date()

    # Check for duplicate records (same sku_id + store_id)
    seen_keys = {}
    duplicate_rows = []
    for i, row in enumerate(rows):
        key = f"{row.get('sku_id', '')}|{row.get('store_id', '')}"
        if key in seen_keys:
            duplicate_rows.append((i, row, seen_keys[key]))
        else:
            seen_keys[key] = i

    if duplicate_rows:
        dup_details = []
        for idx, row, first_idx in duplicate_rows:
            dup_details.append(
                f"  Row {idx+1}: SKU {row.get('sku_id')} at {row.get('store_id')} "
                f"(duplicate of row {first_idx+1})"
            )
        raise ValueError(
            f"Duplicate data detected: {len(duplicate_rows)} duplicate records found "
            f"in inventory feed (same SKU + Store combination).\n"
            f"Duplicates:\n" + "\n".join(dup_details)
        )

    valid_rows = []
    quarantine_rows = []

    for row in rows:
        rejection_reasons = validate_record(row, execution_date)

        if rejection_reasons:
            quarantine_row = build_quarantine_row(row, rejection_reasons)
            quarantine_rows.append(quarantine_row)
        else:
            valid_rows.append(row)

    total_count = len(rows)
    rejected_count = len(quarantine_rows)

    # Threshold check: if rejected > 10% of total, fail the pipeline
    if total_count > 0 and (rejected_count / total_count) > REJECTION_THRESHOLD:
        affected_details = []
        for qr in quarantine_rows:
            affected_details.append(
                f"  SKU {qr.get('sku_id', 'UNKNOWN')}: {qr['rejection_reason']}"
            )
        raise ValueError(
            f"Rejection threshold exceeded: {rejected_count}/{total_count} records "
            f"rejected ({rejected_count/total_count*100:.1f}%) exceeds "
            f"{REJECTION_THRESHOLD*100:.0f}% threshold.\n"
            f"Affected records:\n" + "\n".join(affected_details)
        )

    # Push valid and quarantine rows to XCom
    context["ti"].xcom_push(key="valid_rows", value=valid_rows)
    context["ti"].xcom_push(key="quarantine_rows", value=quarantine_rows)

    logger.info(
        f"[validate_data] Validation complete: "
        f"{total_count} total, {len(valid_rows)} valid, {rejected_count} rejected"
    )


def load_to_staging(**context):
    """Insert validated rows into BigQuery staging table with partition replacement."""
    rows = context["ti"].xcom_pull(key="valid_rows")

    # Build BigQuery rows with all 17 columns
    bq_rows = [
        {
            "sku_id": r["sku_id"],
            "store_id": r["store_id"],
            "department": r["department"],
            "category": r["category"],
            "product_name": r["product_name"],
            "size": r.get("size", ""),
            "color": r.get("color", ""),
            "unit_cost": float(r["unit_cost"]),
            "retail_price": float(r["retail_price"]),
            "inventory_count": int(r["inventory_count"]),
            "reorder_point": int(r["reorder_point"]),
            "warehouse_id": r["warehouse_id"],
            "last_received_date": r.get("last_received_date", None),
            "last_sold_date": r.get("last_sold_date", None),
            "seasonal_flag": r.get("seasonal_flag", None),
            "last_updated": r["last_updated"],
            "ingestion_timestamp": datetime.utcnow().isoformat(),
        }
        for r in rows
    ]

    hook = BigQueryHook(use_legacy_sql=False)
    client = hook.get_client()

    # Determine distinct last_updated dates in the batch for partition replacement
    distinct_dates = set(r["last_updated"] for r in bq_rows)

    # Delete existing rows for each partition date
    for date in distinct_dates:
        delete_query = f"DELETE FROM `{STAGING_TABLE}` WHERE last_updated = '{date}'"
        client.query(delete_query).result()

    # Insert new rows
    errors = client.insert_rows_json(STAGING_TABLE, bq_rows)
    if errors:
        error_count = len(errors)
        logger.error(
            f"[load_to_staging] Failed to insert {error_count} rows into staging: {errors}"
        )
        raise RuntimeError(
            f"Staging load failed: {error_count} rows could not be inserted. Errors: {errors}"
        )

    logger.info(
        f"[load_to_staging] Loaded {len(bq_rows)} rows into staging "
        f"(partitions replaced: {sorted(distinct_dates)})"
    )


def load_quarantine(**context):
    """Insert rejected records into BigQuery quarantine table, or skip if none."""
    quarantine_rows = context["ti"].xcom_pull(key="quarantine_rows")
    ds = context["ds"]

    # If no rejected records, skip this task
    if not quarantine_rows:
        logger.info("[load_quarantine] No quarantine rows — skipping task")
        raise AirflowSkipException("No rejected records to quarantine")

    # Build BigQuery rows with quarantine schema columns
    bq_rows = []
    for row in quarantine_rows:
        bq_row = {
            "sku_id": row.get("sku_id"),
            "store_id": row.get("store_id"),
            "department": row.get("department"),
            "category": row.get("category"),
            "product_name": row.get("product_name"),
            "inventory_count": int(row["inventory_count"]) if row.get("inventory_count") and str(row["inventory_count"]).strip() else None,
            "retail_price": float(row["retail_price"]) if row.get("retail_price") and str(row["retail_price"]).strip() else None,
            "rejection_reason": row["rejection_reason"],
            "pipeline_execution_date": ds,
            "raw_record": row["raw_record"],
        }
        bq_rows.append(bq_row)

    hook = BigQueryHook(use_legacy_sql=False)
    client = hook.get_client()

    # Partition replacement: DELETE existing rows for this execution date, then INSERT
    delete_query = f"DELETE FROM `{QUARANTINE_TABLE}` WHERE pipeline_execution_date = '{ds}'"
    client.query(delete_query).result()

    # Insert quarantine rows
    errors = client.insert_rows_json(QUARANTINE_TABLE, bq_rows)
    if errors:
        raise RuntimeError(
            f"Failed to quarantine {len(bq_rows)} records. "
            f"BigQuery insert errors: {errors}"
        )

    logger.info(
        f"[load_quarantine] Quarantined {len(bq_rows)} rejected records "
        f"for pipeline_execution_date={ds}"
    )


def compute_department_metrics(**context):
    """Compute department-level aggregations from staging data into inventory_by_department."""
    ds = context["ds"]

    hook = BigQueryHook(use_legacy_sql=False)
    client = hook.get_client()

    # Delete existing rows for current report_date (idempotent partition replacement)
    delete_query = f"DELETE FROM `{DEPT_TABLE}` WHERE report_date = '{ds}'"
    client.query(delete_query).result()

    # Insert department-level aggregations
    insert_query = f"""
    INSERT INTO `{DEPT_TABLE}` (store_id, department, total_skus, total_units, total_retail_value, avg_inventory_per_sku, below_reorder_count, report_date)
    SELECT
        store_id,
        department,
        COUNT(DISTINCT sku_id) AS total_skus,
        SUM(inventory_count) AS total_units,
        SUM(retail_price * inventory_count) AS total_retail_value,
        ROUND(SAFE_DIVIDE(SUM(inventory_count), COUNT(DISTINCT sku_id)), 2) AS avg_inventory_per_sku,
        COUNTIF(inventory_count < reorder_point) AS below_reorder_count,
        '{ds}' AS report_date
    FROM `{STAGING_TABLE}`
    WHERE last_updated = '{ds}'
    GROUP BY store_id, department
    """

    result = client.query(insert_query).result()
    row_count = result.num_dml_affected_rows if result.num_dml_affected_rows is not None else 0

    logger.info(
        f"[compute_department_metrics] Computed department metrics for report_date={ds}: "
        f"{row_count} rows inserted into {DEPT_TABLE}"
    )


def compute_store_summary(**context):
    """Aggregate staging data into store-level inventory summary with reorder warnings."""
    hook = BigQueryHook(use_legacy_sql=False)
    client = hook.get_client()

    # Full table replace: DELETE all existing rows, then INSERT aggregated results
    delete_query = f"DELETE FROM `{SUMMARY_TABLE}` WHERE TRUE"
    client.query(delete_query).result()

    insert_query = f"""
    INSERT INTO `{SUMMARY_TABLE}` (store_id, total_skus, total_units, total_retail_value, total_cost_value, departments_represented, skus_below_reorder, last_updated)
    SELECT
        store_id,
        COUNT(DISTINCT sku_id) AS total_skus,
        SUM(inventory_count) AS total_units,
        SUM(retail_price * inventory_count) AS total_retail_value,
        SUM(unit_cost * inventory_count) AS total_cost_value,
        COUNT(DISTINCT department) AS departments_represented,
        COUNTIF(inventory_count < reorder_point) AS skus_below_reorder,
        MAX(last_updated) AS last_updated
    FROM `{STAGING_TABLE}`
    GROUP BY store_id
    HAVING COUNT(DISTINCT sku_id) > 0
    """
    client.query(insert_query).result()

    # Query results to check reorder warning threshold and get row count
    check_query = f"""
    SELECT store_id, total_skus, skus_below_reorder
    FROM `{SUMMARY_TABLE}`
    """
    results = client.query(check_query).result()

    row_count = 0
    for row in results:
        row_count += 1
        total_skus = row["total_skus"]
        skus_below_reorder = row["skus_below_reorder"]
        if total_skus > 0 and (skus_below_reorder / total_skus) > 0.20:
            pct = (skus_below_reorder / total_skus) * 100
            logger.warning(
                f"[compute_store_summary] Store {row['store_id']} has "
                f"{pct:.1f}% of SKUs below reorder point"
            )

    logger.info(
        f"[compute_store_summary] Store summary updated with {row_count} rows"
    )


def compute_price_band_metrics(**context):
    """Classify SKUs into price bands and compute aggregations per store+band."""
    ds = context["ds"]

    hook = BigQueryHook(use_legacy_sql=False)
    client = hook.get_client()

    query = f"""
    DELETE FROM `{PRICE_BAND_TABLE}` WHERE report_date = '{ds}';

    INSERT INTO `{PRICE_BAND_TABLE}` (store_id, price_band, sku_count, total_units, total_retail_value, report_date)
    SELECT
        store_id,
        CASE
            WHEN retail_price <= 50 THEN 'Budget'
            WHEN retail_price <= 200 THEN 'Mid'
            WHEN retail_price <= 500 THEN 'Premium'
            ELSE 'Luxury'
        END AS price_band,
        COUNT(DISTINCT sku_id) AS sku_count,
        SUM(inventory_count) AS total_units,
        SUM(retail_price * inventory_count) AS total_retail_value,
        '{ds}' AS report_date
    FROM `{STAGING_TABLE}`
    WHERE last_updated = '{ds}'
    GROUP BY store_id, price_band
    """

    job = client.query(query)
    job.result()

    # Count output rows inserted for this report_date
    count_query = f"SELECT COUNT(*) AS cnt FROM `{PRICE_BAND_TABLE}` WHERE report_date = '{ds}'"
    count_result = client.query(count_query).result()
    row_count = next(iter(count_result)).cnt

    logger.info(
        f"[compute_price_band_metrics] Inserted {row_count} price band rows "
        f"for report_date={ds}"
    )


with DAG(
    dag_id="daily_inventory_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["retail", "inventory", "demo"],
) as dag:

    with TaskGroup("ingestion") as ingestion:
        load_csv = PythonOperator(
            task_id="load_csv_from_gcs",
            python_callable=load_csv_from_gcs,
        )

    with TaskGroup("validation") as validation:
        validate = PythonOperator(
            task_id="validate_data",
            python_callable=validate_data,
        )

    with TaskGroup("staging") as staging:
        stage = PythonOperator(
            task_id="load_to_staging",
            python_callable=load_to_staging,
        )

        quarantine = PythonOperator(
            task_id="load_quarantine",
            python_callable=load_quarantine,
        )

    with TaskGroup("analytics") as analytics:
        dept_metrics = PythonOperator(
            task_id="compute_department_metrics",
            python_callable=compute_department_metrics,
            trigger_rule="none_failed_min_one_success",
        )

        store_summary = PythonOperator(
            task_id="compute_store_summary",
            python_callable=compute_store_summary,
        )

        price_band = PythonOperator(
            task_id="compute_price_band_metrics",
            python_callable=compute_price_band_metrics,
        )

        dept_metrics >> store_summary >> price_band

    ingestion >> validation >> staging >> analytics

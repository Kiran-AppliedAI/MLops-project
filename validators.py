"""
Validation functions for the Daily Inventory Pipeline.

Extracted from inventory_pipeline_dag.py to enable unit testing and reuse.
Each validator returns True if the record FAILS (should be rejected).
"""

import re
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple

EXPECTED_COLUMNS = [
    "sku_id", "store_id", "department", "category", "product_name",
    "size", "color", "unit_cost", "retail_price", "inventory_count",
    "reorder_point", "warehouse_id", "last_received_date", "last_sold_date",
    "seasonal_flag", "last_updated",
]

REQUIRED_FIELDS = [
    "sku_id", "store_id", "department", "category", "product_name",
    "unit_cost", "retail_price", "inventory_count", "reorder_point",
    "warehouse_id", "last_updated",
]

SKU_PATTERN = re.compile(r"^[A-Z]{3}-[A-Z]{3}-\d{6}$")
STORE_ID_PATTERN = re.compile(r"^[A-Z]{2}[A-Z]{3}-\d{3}$")
REJECTION_THRESHOLD = 0.10


def check_negative_inventory(row: Dict[str, Any]) -> bool:
    """Returns True if inventory_count is negative (record should be rejected)."""
    try:
        return int(row["inventory_count"]) < 0
    except (ValueError, TypeError):
        return False


def check_invalid_margin(row: Dict[str, Any]) -> bool:
    """Returns True if margin is invalid: unit_cost <= 0, retail_price <= 0, or unit_cost >= retail_price."""
    try:
        unit_cost = float(row["unit_cost"])
        retail_price = float(row["retail_price"])
        return unit_cost <= 0 or retail_price <= 0 or unit_cost >= retail_price
    except (ValueError, TypeError):
        return False


def check_malformed_sku(row: Dict[str, Any]) -> bool:
    """Returns True if sku_id does not match ^[A-Z]{3}-[A-Z]{3}-\\d{6}$."""
    sku_id = row.get("sku_id", "")
    return not SKU_PATTERN.match(str(sku_id))


def check_malformed_store_id(row: Dict[str, Any]) -> bool:
    """Returns True if store_id does not match ^[A-Z]{2}[A-Z]{3}-\\d{3}$."""
    store_id = row.get("store_id", "")
    return not STORE_ID_PATTERN.match(str(store_id))


def check_stale_data(row: Dict[str, Any], execution_date) -> bool:
    """Returns True if last_updated is more than 7 days before execution_date."""
    try:
        last_updated = datetime.strptime(row["last_updated"], "%Y-%m-%d").date()
        return (execution_date - last_updated) > timedelta(days=7)
    except (ValueError, TypeError, KeyError):
        return False


def check_implausible_quantity(row: Dict[str, Any]) -> bool:
    """Returns True if inventory_count > 10000."""
    try:
        return int(row["inventory_count"]) > 10000
    except (ValueError, TypeError):
        return False


def check_missing_required_field(row: Dict[str, Any]) -> bool:
    """Returns True if any of the 11 required fields is null or empty."""
    return any(
        row.get(field) is None or str(row.get(field, "")).strip() == ""
        for field in REQUIRED_FIELDS
    )


def validate_record(row: Dict[str, Any], execution_date) -> List[str]:
    """
    Validate a single record against all 7 business rules.
    Returns a list of rejection reason strings (empty if record is valid).
    """
    rejection_reasons = []

    if check_missing_required_field(row):
        rejection_reasons.append("missing_required_field")

    if check_negative_inventory(row):
        rejection_reasons.append("negative_inventory")

    if check_invalid_margin(row):
        rejection_reasons.append("invalid_margin")

    if check_malformed_sku(row):
        rejection_reasons.append("malformed_sku")

    if check_malformed_store_id(row):
        rejection_reasons.append("malformed_store_id")

    if check_stale_data(row, execution_date):
        rejection_reasons.append("stale_data")

    if check_implausible_quantity(row):
        rejection_reasons.append("implausible_quantity")

    return rejection_reasons


def build_quarantine_row(row: Dict[str, Any], rejection_reasons: List[str]) -> Dict[str, Any]:
    """
    Build a quarantine row from the original record and its rejection reasons.
    raw_record is a comma-separated string of the original values in column order.
    """
    raw_record = ",".join(str(row.get(col, "")) for col in EXPECTED_COLUMNS)
    quarantine_row = dict(row)
    quarantine_row["rejection_reason"] = ",".join(rejection_reasons)
    quarantine_row["raw_record"] = raw_record
    return quarantine_row


def validate_batch(rows: List[Dict[str, Any]], execution_date) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Validate a batch of records. Returns (valid_rows, quarantine_rows).
    Raises ValueError if rejection threshold is exceeded or duplicate records detected.
    """
    # Check for duplicate records (same sku_id + store_id combination)
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

    return valid_rows, quarantine_rows

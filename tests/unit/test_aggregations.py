"""
Unit tests for aggregation logic used in the inventory pipeline analytics layer.

These tests verify the computation formulas in pure Python (mirroring the SQL logic
executed in BigQuery) rather than testing BigQuery integration.

Validates: Requirements 5.1, 5.3, 6.1, 7.1
"""

import pytest
from collections import defaultdict
from decimal import Decimal


# --- Pure Python implementations of aggregation logic (mirrors SQL in DAG) ---


def classify_price_band(retail_price: float) -> str:
    """Classify a retail price into a price band.

    Logic mirrors the SQL CASE expression in compute_price_band_metrics:
        WHEN retail_price <= 50 THEN 'Budget'
        WHEN retail_price <= 200 THEN 'Mid'
        WHEN retail_price <= 500 THEN 'Premium'
        ELSE 'Luxury'
    """
    if retail_price <= 50:
        return "Budget"
    elif retail_price <= 200:
        return "Mid"
    elif retail_price <= 500:
        return "Premium"
    else:
        return "Luxury"


def compute_department_aggregation(records: list[dict]) -> dict:
    """Compute department-level metrics from a list of inventory records.

    Groups by (store_id, department) and computes:
    - total_skus: COUNT(DISTINCT sku_id)
    - total_units: SUM(inventory_count)
    - total_retail_value: SUM(retail_price * inventory_count)
    - avg_inventory_per_sku: ROUND(total_units / total_skus, 2)
    - below_reorder_count: COUNTIF(inventory_count < reorder_point)
    """
    groups = defaultdict(list)
    for r in records:
        key = (r["store_id"], r["department"])
        groups[key].append(r)

    results = {}
    for key, group_records in groups.items():
        distinct_skus = set(r["sku_id"] for r in group_records)
        total_skus = len(distinct_skus)
        total_units = sum(int(r["inventory_count"]) for r in group_records)
        total_retail_value = sum(
            float(r["retail_price"]) * int(r["inventory_count"]) for r in group_records
        )
        avg_inventory_per_sku = round(total_units / total_skus, 2) if total_skus > 0 else 0
        below_reorder_count = sum(
            1 for r in group_records if int(r["inventory_count"]) < int(r["reorder_point"])
        )

        results[key] = {
            "store_id": key[0],
            "department": key[1],
            "total_skus": total_skus,
            "total_units": total_units,
            "total_retail_value": total_retail_value,
            "avg_inventory_per_sku": avg_inventory_per_sku,
            "below_reorder_count": below_reorder_count,
        }

    return results


def compute_store_summary(records: list[dict]) -> dict:
    """Compute store-level summary from a list of inventory records.

    Groups by store_id and computes:
    - total_skus: COUNT(DISTINCT sku_id)
    - total_units: SUM(inventory_count)
    - total_retail_value: SUM(retail_price * inventory_count)
    - total_cost_value: SUM(unit_cost * inventory_count)
    - departments_represented: COUNT(DISTINCT department)
    - skus_below_reorder: COUNTIF(inventory_count < reorder_point)
    """
    groups = defaultdict(list)
    for r in records:
        groups[r["store_id"]].append(r)

    results = {}
    for store_id, group_records in groups.items():
        distinct_skus = set(r["sku_id"] for r in group_records)
        total_skus = len(distinct_skus)

        if total_skus == 0:
            continue

        total_units = sum(int(r["inventory_count"]) for r in group_records)
        total_retail_value = sum(
            float(r["retail_price"]) * int(r["inventory_count"]) for r in group_records
        )
        total_cost_value = sum(
            float(r["unit_cost"]) * int(r["inventory_count"]) for r in group_records
        )
        departments_represented = len(set(r["department"] for r in group_records))
        skus_below_reorder = sum(
            1 for r in group_records if int(r["inventory_count"]) < int(r["reorder_point"])
        )

        results[store_id] = {
            "store_id": store_id,
            "total_skus": total_skus,
            "total_units": total_units,
            "total_retail_value": total_retail_value,
            "total_cost_value": total_cost_value,
            "departments_represented": departments_represented,
            "skus_below_reorder": skus_below_reorder,
        }

    return results


def should_warn_reorder(skus_below_reorder: int, total_skus: int) -> bool:
    """Determine if a store should trigger a reorder warning.

    Warning threshold: skus_below_reorder / total_skus > 0.20
    """
    if total_skus == 0:
        return False
    return (skus_below_reorder / total_skus) > 0.20


# --- Test Fixtures ---


def _make_record(
    sku_id="WMN-DRS-000001",
    store_id="NYNYC-001",
    department="WMN",
    unit_cost="45.00",
    retail_price="89.99",
    inventory_count="100",
    reorder_point="25",
):
    """Helper to create a minimal inventory record dict."""
    return {
        "sku_id": sku_id,
        "store_id": store_id,
        "department": department,
        "category": "DRS",
        "product_name": "Test Product",
        "size": "M",
        "color": "Blue",
        "unit_cost": unit_cost,
        "retail_price": retail_price,
        "inventory_count": inventory_count,
        "reorder_point": reorder_point,
        "warehouse_id": "WH-EAST-01",
        "last_received_date": "2026-03-05",
        "last_sold_date": "2026-03-09",
        "seasonal_flag": "N",
        "last_updated": "2026-03-10",
    }


# --- Price Band Classification Tests (Requirement 7.1) ---


class TestPriceBandClassification:
    """Test price band boundary values.

    Price bands:
    - Budget: $0 < price <= $50
    - Mid: $50 < price <= $200
    - Premium: $200 < price <= $500
    - Luxury: price > $500
    """

    def test_budget_upper_boundary(self):
        """$50.00 is the upper boundary of Budget band (inclusive)."""
        assert classify_price_band(50.00) == "Budget"

    def test_mid_lower_boundary(self):
        """$50.01 is just above Budget, so it falls in Mid band."""
        assert classify_price_band(50.01) == "Mid"

    def test_mid_upper_boundary(self):
        """$200.00 is the upper boundary of Mid band (inclusive)."""
        assert classify_price_band(200.00) == "Mid"

    def test_premium_lower_boundary(self):
        """$200.01 is just above Mid, so it falls in Premium band."""
        assert classify_price_band(200.01) == "Premium"

    def test_premium_upper_boundary(self):
        """$500.00 is the upper boundary of Premium band (inclusive)."""
        assert classify_price_band(500.00) == "Premium"

    def test_luxury_lower_boundary(self):
        """$500.01 is just above Premium, so it falls in Luxury band."""
        assert classify_price_band(500.01) == "Luxury"

    def test_minimum_price_is_budget(self):
        """$0.01 (minimum valid price) should be Budget."""
        assert classify_price_band(0.01) == "Budget"


# --- Department Aggregation Tests (Requirements 5.1, 5.3) ---


class TestDepartmentAggregation:
    """Test department-level aggregation logic."""

    def test_total_skus_counts_distinct(self):
        """total_skus = count of distinct sku_ids per (store, department)."""
        records = [
            _make_record(sku_id="WMN-DRS-000001", store_id="NYNYC-001", department="WMN"),
            _make_record(sku_id="WMN-DRS-000002", store_id="NYNYC-001", department="WMN"),
            _make_record(sku_id="WMN-DRS-000001", store_id="NYNYC-001", department="WMN"),  # duplicate SKU
        ]
        result = compute_department_aggregation(records)
        key = ("NYNYC-001", "WMN")
        assert result[key]["total_skus"] == 2  # 2 distinct SKUs

    def test_below_reorder_count(self):
        """below_reorder_count = records where inventory_count < reorder_point."""
        records = [
            _make_record(inventory_count="10", reorder_point="25"),  # below
            _make_record(sku_id="WMN-DRS-000002", inventory_count="30", reorder_point="25"),  # above
            _make_record(sku_id="WMN-DRS-000003", inventory_count="5", reorder_point="25"),  # below
        ]
        result = compute_department_aggregation(records)
        key = ("NYNYC-001", "WMN")
        assert result[key]["below_reorder_count"] == 2

    def test_avg_inventory_per_sku(self):
        """avg_inventory_per_sku = total_units / total_skus rounded to 2 decimals."""
        records = [
            _make_record(sku_id="WMN-DRS-000001", inventory_count="100"),
            _make_record(sku_id="WMN-DRS-000002", inventory_count="50"),
            _make_record(sku_id="WMN-DRS-000003", inventory_count="75"),
        ]
        result = compute_department_aggregation(records)
        key = ("NYNYC-001", "WMN")
        # total_units = 225, total_skus = 3, avg = 225/3 = 75.0
        assert result[key]["avg_inventory_per_sku"] == 75.0

    def test_avg_inventory_per_sku_rounded(self):
        """avg_inventory_per_sku rounds to 2 decimal places."""
        records = [
            _make_record(sku_id="WMN-DRS-000001", inventory_count="100"),
            _make_record(sku_id="WMN-DRS-000002", inventory_count="50"),
            _make_record(sku_id="WMN-DRS-000003", inventory_count="33"),
        ]
        result = compute_department_aggregation(records)
        key = ("NYNYC-001", "WMN")
        # total_units = 183, total_skus = 3, avg = 61.0
        assert result[key]["avg_inventory_per_sku"] == 61.0

    def test_total_units_sums_inventory_count(self):
        """total_units = sum of inventory_count for the group."""
        records = [
            _make_record(sku_id="WMN-DRS-000001", inventory_count="100"),
            _make_record(sku_id="WMN-DRS-000002", inventory_count="200"),
        ]
        result = compute_department_aggregation(records)
        key = ("NYNYC-001", "WMN")
        assert result[key]["total_units"] == 300

    def test_total_retail_value(self):
        """total_retail_value = sum of (retail_price * inventory_count)."""
        records = [
            _make_record(sku_id="WMN-DRS-000001", retail_price="89.99", inventory_count="10"),
            _make_record(sku_id="WMN-DRS-000002", retail_price="120.00", inventory_count="5"),
        ]
        result = compute_department_aggregation(records)
        key = ("NYNYC-001", "WMN")
        expected = (89.99 * 10) + (120.00 * 5)  # 899.90 + 600.00 = 1499.90
        assert result[key]["total_retail_value"] == pytest.approx(expected)

    def test_groups_by_store_and_department(self):
        """Records are grouped by (store_id, department) independently."""
        records = [
            _make_record(sku_id="WMN-DRS-000001", store_id="NYNYC-001", department="WMN", inventory_count="100"),
            _make_record(sku_id="MEN-SHT-000001", store_id="NYNYC-001", department="MEN", inventory_count="50"),
            _make_record(sku_id="WMN-DRS-000002", store_id="CASFO-002", department="WMN", inventory_count="75"),
        ]
        result = compute_department_aggregation(records)
        assert ("NYNYC-001", "WMN") in result
        assert ("NYNYC-001", "MEN") in result
        assert ("CASFO-002", "WMN") in result
        assert result[("NYNYC-001", "WMN")]["total_units"] == 100
        assert result[("NYNYC-001", "MEN")]["total_units"] == 50
        assert result[("CASFO-002", "WMN")]["total_units"] == 75


# --- Store Summary Tests (Requirement 6.1) ---


class TestStoreSummary:
    """Test store-level summary aggregation logic."""

    def test_departments_represented(self):
        """departments_represented = count distinct departments."""
        records = [
            _make_record(sku_id="WMN-DRS-000001", department="WMN"),
            _make_record(sku_id="MEN-SHT-000001", department="MEN"),
            _make_record(sku_id="KID-TOP-000001", department="KID"),
        ]
        result = compute_store_summary(records)
        assert result["NYNYC-001"]["departments_represented"] == 3

    def test_skus_below_reorder(self):
        """skus_below_reorder = total count where inventory_count < reorder_point."""
        records = [
            _make_record(sku_id="WMN-DRS-000001", inventory_count="10", reorder_point="25"),  # below
            _make_record(sku_id="WMN-DRS-000002", inventory_count="30", reorder_point="25"),  # above
            _make_record(sku_id="WMN-DRS-000003", inventory_count="24", reorder_point="25"),  # below
            _make_record(sku_id="WMN-DRS-000004", inventory_count="25", reorder_point="25"),  # equal (NOT below)
        ]
        result = compute_store_summary(records)
        assert result["NYNYC-001"]["skus_below_reorder"] == 2

    def test_total_retail_value(self):
        """total_retail_value = sum of (retail_price * inventory_count)."""
        records = [
            _make_record(sku_id="WMN-DRS-000001", retail_price="100.00", inventory_count="10"),
            _make_record(sku_id="WMN-DRS-000002", retail_price="50.00", inventory_count="20"),
        ]
        result = compute_store_summary(records)
        # 100*10 + 50*20 = 1000 + 1000 = 2000
        assert result["NYNYC-001"]["total_retail_value"] == pytest.approx(2000.00)

    def test_total_cost_value(self):
        """total_cost_value = sum of (unit_cost * inventory_count)."""
        records = [
            _make_record(sku_id="WMN-DRS-000001", unit_cost="45.00", inventory_count="10"),
            _make_record(sku_id="WMN-DRS-000002", unit_cost="30.00", inventory_count="20"),
        ]
        result = compute_store_summary(records)
        # 45*10 + 30*20 = 450 + 600 = 1050
        assert result["NYNYC-001"]["total_cost_value"] == pytest.approx(1050.00)

    def test_total_skus_counts_distinct(self):
        """total_skus = count of distinct sku_id per store."""
        records = [
            _make_record(sku_id="WMN-DRS-000001"),
            _make_record(sku_id="WMN-DRS-000002"),
            _make_record(sku_id="WMN-DRS-000001"),  # duplicate
        ]
        result = compute_store_summary(records)
        assert result["NYNYC-001"]["total_skus"] == 2

    def test_omits_stores_with_no_records(self):
        """Stores with zero distinct SKUs are omitted from summary."""
        records = [
            _make_record(sku_id="WMN-DRS-000001", store_id="NYNYC-001"),
        ]
        result = compute_store_summary(records)
        assert "NYNYC-001" in result
        assert "CASFO-002" not in result


# --- Reorder Warning Threshold Tests (Requirement 6.1, Design Property 11) ---


class TestReorderWarningThreshold:
    """Test reorder warning threshold logic.

    Warning fires when skus_below_reorder / total_skus > 0.20 (more than 20%).
    """

    def test_30_percent_below_reorder_should_warn(self):
        """3/10 SKUs below reorder (30%) → should warn."""
        assert should_warn_reorder(skus_below_reorder=3, total_skus=10) is True

    def test_20_percent_below_reorder_should_not_warn(self):
        """2/10 SKUs below reorder (20%) → should NOT warn (threshold is >20%)."""
        assert should_warn_reorder(skus_below_reorder=2, total_skus=10) is False

    def test_exactly_20_percent_boundary(self):
        """Exactly 20% (e.g., 1/5) should NOT warn — threshold is strictly >20%."""
        assert should_warn_reorder(skus_below_reorder=1, total_skus=5) is False

    def test_21_percent_should_warn(self):
        """Just above 20% should warn."""
        # 21/100 = 21% > 20%
        assert should_warn_reorder(skus_below_reorder=21, total_skus=100) is True

    def test_zero_total_skus_should_not_warn(self):
        """Zero total SKUs means no warning (avoid division by zero)."""
        assert should_warn_reorder(skus_below_reorder=0, total_skus=0) is False

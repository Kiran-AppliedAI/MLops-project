"""
Unit tests for inventory pipeline validation rules.

Tests each of the 7 validators with accept/reject examples, multi-rule failure
collection, batch rejection threshold, and quarantine row construction.

Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9
"""

import sys
import os
from datetime import date, timedelta

import pytest

# Add project root to path so we can import validators
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from validators import (
    check_negative_inventory,
    check_invalid_margin,
    check_malformed_sku,
    check_malformed_store_id,
    check_stale_data,
    check_implausible_quantity,
    check_missing_required_field,
    validate_record,
    validate_batch,
    build_quarantine_row,
    EXPECTED_COLUMNS,
    REQUIRED_FIELDS,
)


def _make_valid_row(overrides=None):
    """Create a valid record with all required fields populated."""
    row = {
        "sku_id": "WMN-DRS-004521",
        "store_id": "NYNYC-001",
        "department": "WMN",
        "category": "DRS",
        "product_name": "Classic Fit Oxford Shirt",
        "size": "M",
        "color": "Navy",
        "unit_cost": "45.00",
        "retail_price": "89.99",
        "inventory_count": "142",
        "reorder_point": "25",
        "warehouse_id": "WH-EAST-01",
        "last_received_date": "2026-03-05",
        "last_sold_date": "2026-03-09",
        "seasonal_flag": "N",
        "last_updated": "2026-03-10",
    }
    if overrides:
        row.update(overrides)
    return row


# ============================================================
# Test: negative_inventory validator
# Requirement 2.1
# ============================================================

class TestNegativeInventory:
    def test_reject_negative_count(self):
        """inventory_count = -1 should be rejected."""
        row = _make_valid_row({"inventory_count": "-1"})
        assert check_negative_inventory(row) is True

    def test_accept_zero_count(self):
        """inventory_count = 0 should be accepted."""
        row = _make_valid_row({"inventory_count": "0"})
        assert check_negative_inventory(row) is False

    def test_accept_positive_count(self):
        """inventory_count = 5 should be accepted."""
        row = _make_valid_row({"inventory_count": "5"})
        assert check_negative_inventory(row) is False


# ============================================================
# Test: invalid_margin validator
# Requirement 2.2
# ============================================================

class TestInvalidMargin:
    def test_reject_cost_greater_than_price(self):
        """unit_cost=89.99, retail_price=45.00 should be rejected."""
        row = _make_valid_row({"unit_cost": "89.99", "retail_price": "45.00"})
        assert check_invalid_margin(row) is True

    def test_reject_cost_equal_to_price(self):
        """unit_cost=45, retail_price=45 should be rejected."""
        row = _make_valid_row({"unit_cost": "45", "retail_price": "45"})
        assert check_invalid_margin(row) is True

    def test_accept_valid_margin(self):
        """unit_cost=30, retail_price=89.99 should be accepted."""
        row = _make_valid_row({"unit_cost": "30", "retail_price": "89.99"})
        assert check_invalid_margin(row) is False


# ============================================================
# Test: malformed_sku validator
# Requirement 2.3
# ============================================================

class TestMalformedSku:
    def test_reject_invalid_sku(self):
        """'INVALID-SKU' should be rejected."""
        row = _make_valid_row({"sku_id": "INVALID-SKU"})
        assert check_malformed_sku(row) is True

    def test_accept_valid_sku(self):
        """'WMN-DRS-004521' should be accepted."""
        row = _make_valid_row({"sku_id": "WMN-DRS-004521"})
        assert check_malformed_sku(row) is False

    def test_reject_short_segments(self):
        """'WM-DR-004521' should be rejected (2-letter segments instead of 3)."""
        row = _make_valid_row({"sku_id": "WM-DR-004521"})
        assert check_malformed_sku(row) is True


# ============================================================
# Test: malformed_store_id validator
# Requirement 2.4
# ============================================================

class TestMalformedStoreId:
    def test_reject_invalid_store_id(self):
        """'NY-001' should be rejected (too short)."""
        row = _make_valid_row({"store_id": "NY-001"})
        assert check_malformed_store_id(row) is True

    def test_accept_valid_store_id(self):
        """'NYNYC-001' should be accepted."""
        row = _make_valid_row({"store_id": "NYNYC-001"})
        assert check_malformed_store_id(row) is False

    def test_reject_leading_digit(self):
        """'1YNYC-001' should be rejected (starts with digit)."""
        row = _make_valid_row({"store_id": "1YNYC-001"})
        assert check_malformed_store_id(row) is True


# ============================================================
# Test: stale_data validator
# Requirement 2.5
# ============================================================

class TestStaleData:
    def test_reject_20_days_old(self):
        """Date 20 days ago should be rejected."""
        execution_date = date(2026, 3, 10)
        stale_date = execution_date - timedelta(days=20)
        row = _make_valid_row({"last_updated": stale_date.isoformat()})
        assert check_stale_data(row, execution_date) is True

    def test_accept_3_days_old(self):
        """Date 3 days ago should be accepted."""
        execution_date = date(2026, 3, 10)
        recent_date = execution_date - timedelta(days=3)
        row = _make_valid_row({"last_updated": recent_date.isoformat()})
        assert check_stale_data(row, execution_date) is False

    def test_reject_8_days_old(self):
        """Date 8 days ago should be rejected (> 7 days)."""
        execution_date = date(2026, 3, 10)
        stale_date = execution_date - timedelta(days=8)
        row = _make_valid_row({"last_updated": stale_date.isoformat()})
        assert check_stale_data(row, execution_date) is True


# ============================================================
# Test: implausible_quantity validator
# Requirement 2.6
# ============================================================

class TestImplausibleQuantity:
    def test_reject_over_10000(self):
        """inventory_count=10001 should be rejected."""
        row = _make_valid_row({"inventory_count": "10001"})
        assert check_implausible_quantity(row) is True

    def test_accept_exactly_10000(self):
        """inventory_count=10000 should be accepted (boundary)."""
        row = _make_valid_row({"inventory_count": "10000"})
        assert check_implausible_quantity(row) is False

    def test_accept_5000(self):
        """inventory_count=5000 should be accepted."""
        row = _make_valid_row({"inventory_count": "5000"})
        assert check_implausible_quantity(row) is False


# ============================================================
# Test: missing_required_field validator
# Requirement 2.7
# ============================================================

class TestMissingRequiredField:
    def test_reject_empty_sku_id(self):
        """Empty sku_id should be rejected."""
        row = _make_valid_row({"sku_id": ""})
        assert check_missing_required_field(row) is True

    def test_accept_all_fields_present(self):
        """Record with all required fields populated should be accepted."""
        row = _make_valid_row()
        assert check_missing_required_field(row) is False


# ============================================================
# Test: Multi-rule failure collection
# Requirement 2.8
# ============================================================

class TestMultiRuleFailure:
    def test_multiple_reasons_collected(self):
        """Record with negative count AND invalid margin gets BOTH reasons."""
        execution_date = date(2026, 3, 10)
        row = _make_valid_row({
            "inventory_count": "-1",
            "unit_cost": "89.99",
            "retail_price": "45.00",
        })
        reasons = validate_record(row, execution_date)
        assert "negative_inventory" in reasons
        assert "invalid_margin" in reasons
        assert len(reasons) >= 2


# ============================================================
# Test: Threshold boundary
# Requirement 2.9
# ============================================================

class TestThresholdBoundary:
    def test_exactly_10_percent_does_not_trigger(self):
        """Exactly 10% rejected (1 out of 10) should NOT raise."""
        execution_date = date(2026, 3, 10)
        # 9 valid records + 1 invalid record = 10% rejection rate
        rows = [_make_valid_row() for _ in range(9)]
        rows.append(_make_valid_row({"inventory_count": "-1"}))

        # Should not raise - 10% is equal to threshold, not exceeding it
        valid, quarantine = validate_batch(rows, execution_date)
        assert len(quarantine) == 1
        assert len(valid) == 9

    def test_11_percent_triggers_threshold(self):
        """More than 10% rejected should raise ValueError."""
        execution_date = date(2026, 3, 10)
        # 8 valid + 2 invalid = 20% > 10% threshold
        rows = [_make_valid_row() for _ in range(8)]
        rows.append(_make_valid_row({"inventory_count": "-1"}))
        rows.append(_make_valid_row({"inventory_count": "-2"}))

        with pytest.raises(ValueError, match="Rejection threshold exceeded"):
            validate_batch(rows, execution_date)


# ============================================================
# Test: Quarantine row construction
# Requirement 3.1, 3.2
# ============================================================

class TestQuarantineRowConstruction:
    def test_raw_record_is_comma_separated_values(self):
        """raw_record should be comma-separated original values in column order."""
        row = _make_valid_row({"inventory_count": "-1"})
        reasons = ["negative_inventory"]
        quarantine_row = build_quarantine_row(row, reasons)

        # Verify raw_record is comma-separated values in EXPECTED_COLUMNS order
        expected_parts = [str(row.get(col, "")) for col in EXPECTED_COLUMNS]
        expected_raw = ",".join(expected_parts)
        assert quarantine_row["raw_record"] == expected_raw

    def test_rejection_reason_is_comma_separated(self):
        """rejection_reason should be comma-separated list of reasons."""
        row = _make_valid_row()
        reasons = ["negative_inventory", "invalid_margin"]
        quarantine_row = build_quarantine_row(row, reasons)
        assert quarantine_row["rejection_reason"] == "negative_inventory,invalid_margin"

    def test_quarantine_row_preserves_original_fields(self):
        """Quarantine row should contain all original row fields."""
        row = _make_valid_row({"inventory_count": "-1"})
        reasons = ["negative_inventory"]
        quarantine_row = build_quarantine_row(row, reasons)

        # All original fields preserved
        for key, value in row.items():
            assert quarantine_row[key] == value

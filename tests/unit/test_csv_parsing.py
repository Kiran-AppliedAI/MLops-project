"""
Unit tests for CSV parsing logic in load_csv_from_gcs.

Validates: Requirements 1.1, 9.4

Since Airflow is not installed in the test environment, we mock the airflow
modules at the sys.modules level before importing the DAG module. We then
patch GCSHook to return controlled CSV content.
"""

import sys
import pytest
from unittest.mock import patch, MagicMock

# Mock airflow and its submodules before importing the DAG
_airflow_mocks = {}
for mod_name in [
    "airflow",
    "airflow.models",
    "airflow.operators",
    "airflow.operators.python",
    "airflow.providers",
    "airflow.providers.google",
    "airflow.providers.google.cloud",
    "airflow.providers.google.cloud.hooks",
    "airflow.providers.google.cloud.hooks.gcs",
    "airflow.providers.google.cloud.hooks.bigquery",
    "airflow.exceptions",
    "airflow.utils",
    "airflow.utils.task_group",
]:
    mock = MagicMock()
    _airflow_mocks[mod_name] = mock
    sys.modules[mod_name] = mock

# Set up the specific mocks needed for DAG module import
sys.modules["airflow"].DAG = MagicMock()
sys.modules["airflow.operators.python"].PythonOperator = MagicMock()
sys.modules["airflow.providers.google.cloud.hooks.gcs"].GCSHook = MagicMock()
sys.modules["airflow.providers.google.cloud.hooks.bigquery"].BigQueryHook = MagicMock()
sys.modules["airflow.exceptions"].AirflowSkipException = type("AirflowSkipException", (Exception,), {})
sys.modules["airflow.utils.task_group"].TaskGroup = MagicMock()

# Now import the DAG module
import inventory_pipeline_dag
from inventory_pipeline_dag import load_csv_from_gcs, EXPECTED_COLUMNS


def _build_csv(columns, rows):
    """Helper to build a CSV string from column list and list-of-lists of values."""
    lines = [",".join(columns)]
    for row in rows:
        lines.append(",".join(str(v) for v in row))
    return "\n".join(lines)


def _make_valid_row():
    """Return a single valid row as a list of values matching EXPECTED_COLUMNS order."""
    return [
        "WMN-DRS-004521",  # sku_id
        "NYNYC-001",       # store_id
        "WMN",             # department
        "DRS",             # category
        "Classic Fit Oxford Shirt",  # product_name
        "M",               # size
        "Navy",            # color
        "45.00",           # unit_cost
        "89.99",           # retail_price
        "142",             # inventory_count
        "25",              # reorder_point
        "WH-EAST-01",     # warehouse_id
        "2026-03-05",     # last_received_date
        "2026-03-09",     # last_sold_date
        "N",              # seasonal_flag
        "2026-03-10",     # last_updated
    ]


def _invoke_load_csv(csv_content):
    """
    Invoke load_csv_from_gcs with mocked GCSHook returning the given CSV content.
    Returns the rows pushed to XCom.
    """
    with patch.object(inventory_pipeline_dag, "GCSHook") as mock_hook_cls:
        mock_hook = MagicMock()
        mock_hook_cls.return_value = mock_hook
        mock_hook.download.return_value = csv_content.encode("utf-8")

        # Mock Airflow context
        ti_mock = MagicMock()
        pushed_values = {}

        def xcom_push(key, value):
            pushed_values[key] = value

        ti_mock.xcom_push.side_effect = xcom_push

        context = {"ds": "2026-03-10", "ti": ti_mock}

        load_csv_from_gcs(**context)

        return pushed_values.get("inventory_rows")


class TestCorrectHeaderDetection:
    """Test that a CSV with all 16 expected columns parses successfully."""

    def test_valid_16_column_csv_parses(self):
        """A CSV with the correct 16-column header is accepted."""
        row = _make_valid_row()
        csv_content = _build_csv(EXPECTED_COLUMNS, [row])

        rows = _invoke_load_csv(csv_content)

        assert rows is not None
        assert len(rows) == 1
        assert set(rows[0].keys()) == set(EXPECTED_COLUMNS)


class TestMissingColumnsRaisesError:
    """Test that a CSV missing a required column produces a clear error."""

    def test_missing_one_column_raises_valueerror(self):
        """A CSV missing the 'department' column raises ValueError mentioning it."""
        columns_without_dept = [c for c in EXPECTED_COLUMNS if c != "department"]
        row = _make_valid_row()
        # Remove the value at index 2 (department)
        row.pop(2)
        csv_content = _build_csv(columns_without_dept, [row])

        with pytest.raises(ValueError, match="department"):
            _invoke_load_csv(csv_content)

    def test_missing_multiple_columns_raises_valueerror(self):
        """A CSV missing multiple columns mentions missing columns."""
        columns_reduced = [c for c in EXPECTED_COLUMNS if c not in ("sku_id", "last_updated")]
        row = _make_valid_row()
        # Remove sku_id (index 0) and last_updated (last index)
        row.pop(0)
        row.pop(-1)
        csv_content = _build_csv(columns_reduced, [row])

        with pytest.raises(ValueError, match="Missing columns"):
            _invoke_load_csv(csv_content)


class TestExtraColumnsRaisesError:
    """Test that a CSV with unexpected extra columns produces a clear error."""

    def test_extra_column_raises_valueerror(self):
        """A CSV with an extra unexpected column raises ValueError."""
        columns_with_extra = EXPECTED_COLUMNS + ["unexpected_extra"]
        row = _make_valid_row() + ["extra_value"]
        csv_content = _build_csv(columns_with_extra, [row])

        with pytest.raises(ValueError, match="Unexpected columns"):
            _invoke_load_csv(csv_content)


class TestEmptyFileHandling:
    """Test that an empty string or whitespace-only input raises ValueError."""

    def test_empty_string_raises_valueerror(self):
        """An empty file raises ValueError."""
        with pytest.raises(ValueError, match="Empty file"):
            _invoke_load_csv("")

    def test_whitespace_only_raises_valueerror(self):
        """A whitespace-only file raises ValueError."""
        with pytest.raises(ValueError, match="Empty file"):
            _invoke_load_csv("   \n\t\n  ")


class TestZeroDataRows:
    """Test that a file with only a header row (no data) raises ValueError."""

    def test_header_only_raises_valueerror(self):
        """A CSV with only the header row and no data rows raises ValueError."""
        csv_content = ",".join(EXPECTED_COLUMNS)

        with pytest.raises(ValueError, match="No data rows"):
            _invoke_load_csv(csv_content)


class TestValidParse:
    """Test that a properly formatted CSV with multiple rows returns correct dicts."""

    def test_three_rows_returns_three_dicts(self):
        """A valid CSV with 3 data rows returns 3 dicts with correct field names."""
        rows = []
        for i in range(3):
            row = _make_valid_row()
            row[0] = f"WMN-DRS-{100000 + i:06d}"  # unique sku_id
            rows.append(row)

        csv_content = _build_csv(EXPECTED_COLUMNS, rows)
        result = _invoke_load_csv(csv_content)

        assert result is not None
        assert len(result) == 3
        for record in result:
            assert set(record.keys()) == set(EXPECTED_COLUMNS)

    def test_field_values_match_input(self):
        """Parsed dicts contain the correct field values from the CSV."""
        row = _make_valid_row()
        csv_content = _build_csv(EXPECTED_COLUMNS, [row])
        result = _invoke_load_csv(csv_content)

        assert result[0]["sku_id"] == "WMN-DRS-004521"
        assert result[0]["store_id"] == "NYNYC-001"
        assert result[0]["department"] == "WMN"
        assert result[0]["unit_cost"] == "45.00"
        assert result[0]["retail_price"] == "89.99"
        assert result[0]["inventory_count"] == "142"
        assert result[0]["last_updated"] == "2026-03-10"

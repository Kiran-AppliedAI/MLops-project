# Implementation Plan: Macy's Inventory Pipeline

## Overview

This plan converts the existing simple 4-task Airflow DAG into a production-grade multi-store inventory pipeline with 7 validation rules, quarantine handling, 3 analytics layers, and realistic sample data. The implementation uses Python (Airflow 2.x, BigQuery client) and Terraform for infrastructure, with Hypothesis for property-based testing.

## Tasks

- [x] 1. Update BigQuery Terraform schemas
  - [x] 1.1 Replace staging_inventory and inventory_summary table definitions in bigquery.tf
    - Replace the existing `staging_inventory` resource with the full 17-column schema (sku_id, store_id, department, category, product_name, size, color, unit_cost, retail_price, inventory_count, reorder_point, warehouse_id, last_received_date, last_sold_date, seasonal_flag, last_updated, ingestion_timestamp)
    - Add time_partitioning by `last_updated` (type DAY) and clustering on `department, store_id`
    - Replace the existing `inventory_summary` resource with columns: store_id, total_skus, total_units, total_retail_value, total_cost_value, departments_represented, skus_below_reorder, last_updated
    - _Requirements: 11.1, 11.5_

  - [x] 1.2 Add quarantine_records, inventory_by_department, and inventory_by_price_band tables to bigquery.tf
    - Add `quarantine_records` table: sku_id, store_id, department, category, product_name, inventory_count, retail_price, rejection_reason, pipeline_execution_date, raw_record; partitioned by pipeline_execution_date
    - Add `inventory_by_department` table: store_id, department, total_skus, total_units, total_retail_value, avg_inventory_per_sku, below_reorder_count, report_date; partitioned by report_date, clustered by department
    - Add `inventory_by_price_band` table: store_id, price_band, sku_count, total_units, total_retail_value, report_date; partitioned by report_date
    - _Requirements: 11.2, 11.3, 11.4_

- [x] 2. Create realistic sample CSV data files
  - [x] 2.1 Create the happy-path inventory CSV file (inventory_2026_03_10.csv)
    - Replace the existing `inventory_2026_03_10.csv` with 50+ records spanning all 7 departments (min 5 per department)
    - Include realistic product names (2–8 words), proper SKU format, store IDs from 10+ stores across 5+ states
    - Include price ranges per department as specified, 3+ warehouses, seasonal_flag mix (≥20% Y, ≥20% N)
    - inventory_count 0–500, at least 5 records with inventory_count < reorder_point
    - All last_updated dates within 7 days of 2026-03-10
    - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 10.7_

  - [x] 2.2 Create failure scenario CSV files
    - Create `fail_invalid_margin_2026_03_10.csv`: 10+ records, at least 3 with unit_cost >= retail_price
    - Create `fail_malformed_sku_2026_03_10.csv`: 10+ records, at least 3 with invalid SKU format
    - Create `fail_stale_data_2026_03_10.csv`: 10+ records, at least 3 with last_updated > 7 days before execution date
    - Create `fail_threshold_2026_03_10.csv`: 20+ records, >10% failing validation (triggers threshold failure)
    - Create `fail_mixed_quarantine_2026_03_10.csv`: 20+ records, mix of valid/invalid where invalid < 10% (pipeline succeeds, records quarantined)
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7_

- [x] 3. Implement enhanced DAG with validation engine
  - [x] 3.1 Implement load_csv_from_gcs task with full 16-column parsing
    - Replace the existing function with one that parses all 16 CSV columns into dicts
    - Validate CSV header matches expected columns; fail with clear error if mismatched
    - Fail on empty file or file-not-found with descriptive error
    - Push parsed rows to XCom as `inventory_rows`
    - Log input row count
    - _Requirements: 1.1, 9.4_

  - [x] 3.2 Implement validate_data task with 7 validation rules and threshold check
    - Implement all 7 validators: negative_inventory, invalid_margin, malformed_sku, malformed_store_id, stale_data, implausible_quantity, missing_required_field
    - Collect ALL applicable rejection reasons per record (multi-rule failure)
    - Build quarantine rows with rejection_reason (comma-separated) and raw_record (original CSV line)
    - Check rejection threshold: if rejected_count / total_count > 0.10, raise error with rejection_reason and sku_id for each affected record
    - Push `valid_rows` and `quarantine_rows` to XCom
    - Log counts of valid and rejected records
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9, 8.6, 9.4_

  - [x] 3.3 Implement load_to_staging task with partition replacement
    - Insert valid records into `staging_inventory` with all 16 columns plus ingestion_timestamp (UTC now)
    - Implement partition replacement: DELETE existing rows for the distinct last_updated dates in the batch, then INSERT
    - Handle partial failure: if insert fails, clean up partially written rows and raise error with count
    - Log output row count
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 9.4_

  - [x] 3.4 Implement load_quarantine task with skip-when-empty logic
    - Insert quarantine rows into `quarantine_records` with pipeline_execution_date
    - Implement partition replacement for pipeline_execution_date (DELETE + INSERT)
    - Skip task (mark as SKIPPED using AirflowSkipException) if quarantine_rows is empty
    - If insert fails, raise error with count of records that couldn't be quarantined
    - Log quarantine count
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 9.2, 9.4_

  - [x] 3.5 Implement compute_department_metrics analytics task
    - Execute SQL aggregation on staging_inventory grouped by store_id, department
    - Compute: total_skus (COUNT DISTINCT sku_id), total_units (SUM inventory_count), total_retail_value (SUM retail_price × inventory_count), avg_inventory_per_sku (total_units / total_skus rounded to 2 decimals), below_reorder_count
    - Replace data in inventory_by_department for current report_date
    - Log output row count
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 9.4_

  - [x] 3.6 Implement compute_store_summary analytics task
    - Execute SQL aggregation on staging_inventory grouped by store_id
    - Compute: total_skus, total_units, total_retail_value, total_cost_value, departments_represented, skus_below_reorder, last_updated (MAX)
    - Full table replace of inventory_summary
    - Log warning for any store where skus_below_reorder / total_skus > 0.20
    - Omit stores with zero records from summary
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 9.4_

  - [x] 3.7 Implement compute_price_band_metrics analytics task
    - Execute SQL with CASE expression for price band classification: Budget (≤$50), Mid ($50–$200], Premium ($200–$500], Luxury (>$500)
    - Compute per store+price_band: sku_count, total_units, total_retail_value
    - Replace current day's data in inventory_by_price_band
    - Log output row count
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 9.4_

  - [x] 3.8 Wire DAG structure with task groups and dependencies
    - Organize tasks into 4 task groups: ingestion, validation, staging, analytics
    - Set dependency chain: load_csv_from_gcs → validate_data → [load_to_staging, load_quarantine] → compute_department_metrics → compute_store_summary → compute_price_band_metrics
    - Set trigger_rule on compute_department_metrics to `none_failed_min_one_success` to handle quarantine skip
    - _Requirements: 9.1, 9.2, 9.3_

- [x] 4. Checkpoint - Verify DAG structure and data files
  - Ensure all tests pass, ask the user if questions arise.

- [x] 5. Write unit tests
  - [x] 5.1 Create tests/unit/test_validators.py with unit tests for each validation rule
    - Test each of the 7 validators with specific examples (accept and reject cases)
    - Test multi-rule failure collection (record failing 2+ rules gets all reasons)
    - Test threshold boundary: exactly 10% rejected (should NOT trigger), 10.01% (should trigger)
    - Test quarantine row construction (raw_record preserves original CSV line)
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9_

  - [x] 5.2 Create tests/unit/test_aggregations.py with unit tests for aggregation logic
    - Test department aggregation with known input → expected output
    - Test price band boundary values ($50.00, $200.00, $500.00)
    - Test store summary with single record per group, zero inventory, empty groups
    - Test below_reorder_count calculation
    - _Requirements: 5.1, 5.3, 6.1, 7.1_

  - [x] 5.3 Create tests/unit/test_csv_parsing.py with unit tests for CSV parsing
    - Test correct 16-column header detection
    - Test missing columns raises error
    - Test empty file handling
    - _Requirements: 1.1, 9.4_

- [ ] 6. Write property-based tests with Hypothesis
  - [ ]* 6.1 Create tests/property/test_validator_properties.py — Properties 1-4
    - **Property 1: SKU Format Validator Correctness**
    - **Validates: Requirements 1.2, 2.3**
    - **Property 2: Store ID Format Validator Correctness**
    - **Validates: Requirements 1.3, 2.4**
    - **Property 3: Margin Validator Correctness**
    - **Validates: Requirements 1.6, 2.2**
    - **Property 4: Inventory Count Range Validator**
    - **Validates: Requirements 2.1, 2.6**

  - [ ]* 6.2 Create tests/property/test_validator_properties.py — Properties 5-8
    - **Property 5: Staleness Validator Correctness**
    - **Validates: Requirements 2.5**
    - **Property 6: Missing Required Field Validator**
    - **Validates: Requirements 2.7**
    - **Property 7: Multi-Rule Failure Collection**
    - **Validates: Requirements 2.8**
    - **Property 8: Batch Rejection Threshold**
    - **Validates: Requirements 2.9**

  - [ ]* 6.3 Create tests/property/test_aggregation_properties.py — Properties 9-12
    - **Property 9: Department Aggregation Correctness**
    - **Validates: Requirements 5.1, 5.3**
    - **Property 10: Store Summary Correctness**
    - **Validates: Requirements 6.1**
    - **Property 11: Reorder Warning Threshold**
    - **Validates: Requirements 6.3**
    - **Property 12: Price Band Classification Correctness**
    - **Validates: Requirements 7.1**

- [x] 7. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 8. Write integration tests
  - [ ]* 8.1 Create tests/integration/test_bigquery_operations.py
    - Test staging partition replacement: load data twice for same date, verify no duplicates
    - Test quarantine idempotency: run quarantine insert twice for same execution date, verify replaced not appended
    - _Requirements: 4.3, 3.4_

  - [ ]* 8.2 Create tests/integration/test_pipeline_e2e.py
    - Test full pipeline happy path with sample data: verify all 5 tables populated correctly
    - Test pipeline with all-valid data: verify quarantine task is SKIPPED
    - Test each failure scenario CSV triggers expected behavior
    - _Requirements: 9.1, 9.2, 8.1, 8.2, 8.3, 8.4, 8.5_

- [x] 9. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties from the design document using Hypothesis
- Unit tests validate specific examples and edge cases using pytest
- Integration tests require GCP credentials and BigQuery access
- The existing alerting chain (Cloud Monitoring → Pub/Sub → Cloud Function → Jira → OpsRabbit) requires no changes
- Terraform changes in task 1 should be applied (`terraform plan`/`apply`) before running integration tests

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2", "2.1"] },
    { "id": 1, "tasks": ["2.2", "3.1"] },
    { "id": 2, "tasks": ["3.2"] },
    { "id": 3, "tasks": ["3.3", "3.4"] },
    { "id": 4, "tasks": ["3.5", "3.6", "3.7"] },
    { "id": 5, "tasks": ["3.8"] },
    { "id": 6, "tasks": ["5.1", "5.2", "5.3"] },
    { "id": 7, "tasks": ["6.1", "6.2", "6.3"] },
    { "id": 8, "tasks": ["8.1", "8.2"] }
  ]
}
```

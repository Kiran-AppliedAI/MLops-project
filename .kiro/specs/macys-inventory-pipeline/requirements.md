# Requirements Document

## Introduction

This feature enhances the existing demo retail inventory pipeline to reflect the realistic complexity of a major US department store (Macy's-like retailer). The enhanced pipeline processes daily inventory feeds from multiple store locations across departments (clothing, shoes, accessories, home goods, beauty), incorporates realistic SKU naming, price data, warehouse transfers, seasonal indicators, and multi-layered validation rules. It continues to operate within the existing GCP infrastructure (Cloud Composer, BigQuery, GCS) and integrates with the current alerting chain (Cloud Monitoring → Pub/Sub → Cloud Function → Jira → OpsRabbit AI agent).

## Glossary

- **Pipeline**: The Cloud Composer (Airflow) DAG that orchestrates daily inventory data processing
- **Inventory_Feed**: A daily CSV file uploaded to GCS containing inventory records from all stores
- **SKU**: Stock Keeping Unit — a unique product identifier following the format `{DEPT}-{CATEGORY}-{SEQUENCE}` (e.g., `WMN-DRS-004521`)
- **Store**: A physical retail location identified by a code following the format `{STATE}{CITY_CODE}-{NUMBER}` (e.g., `NYNYC-001` for New York City store #1)
- **Warehouse**: A distribution center that supplies stores, identified by region code (e.g., `WH-EAST-01`)
- **Department**: A product category division (Women's, Men's, Kids, Home, Beauty, Shoes, Accessories)
- **Validator**: The pipeline component that checks data quality against business rules
- **Staging_Layer**: BigQuery tables that hold validated daily data before aggregation
- **Analytics_Layer**: BigQuery tables containing aggregated metrics and summaries
- **Quarantine_Table**: A BigQuery table that stores rejected records with rejection reasons
- **Price_Band**: A classification of products by retail price range (Budget: $0–$50, Mid: $50–$200, Premium: $200–$500, Luxury: $500+)
- **Reorder_Point**: The inventory threshold below which a product should be reordered
- **Shrinkage**: Inventory loss due to theft, damage, or administrative errors
- **Seasonal_Flag**: An indicator of whether a product is seasonal (e.g., winter coats, swimwear)

## Requirements

### Requirement 1: Realistic Data Model

**User Story:** As a demo presenter, I want the inventory CSV to reflect a realistic retail data model, so that the demo accurately represents what a major retailer's data pipeline would process.

#### Acceptance Criteria

1. THE Inventory_Feed SHALL contain the following columns: sku_id, store_id, department, category, product_name, size, color, unit_cost, retail_price, inventory_count, reorder_point, warehouse_id, last_received_date, last_sold_date, seasonal_flag, last_updated
2. WHEN a new Inventory_Feed is generated, THE Pipeline SHALL accept SKU identifiers in the format `{DEPT_CODE}-{CAT_CODE}-{6_DIGIT_SEQUENCE}` where DEPT_CODE is a 3-letter department abbreviation and CAT_CODE is a 3-letter category abbreviation
3. WHEN a new Inventory_Feed is generated, THE Pipeline SHALL accept store identifiers in the format `{2_LETTER_STATE}{3_LETTER_CITY}-{3_DIGIT_NUMBER}` (e.g., NYNYC-001, CASFO-002, ILDCH-001)
4. THE Inventory_Feed SHALL include records spanning all 7 defined departments: Women's (WMN), Men's (MEN), Kids (KID), Home (HOM), Beauty (BTY), Shoes (SHO), and Accessories (ACC), with at least 1 record per department
5. THE Inventory_Feed SHALL include records from at least 10 store locations distributed across at least 5 US states, with at least 1 store per state
6. THE Inventory_Feed SHALL include price data where unit_cost is strictly less than retail_price for each record, with a minimum margin of 10% (retail_price >= unit_cost × 1.10), and both values expressed as decimal numbers with exactly 2 decimal places in the range 0.01 to 999999.99
7. THE Inventory_Feed SHALL represent all date columns (last_received_date, last_sold_date, last_updated) in ISO 8601 date format (YYYY-MM-DD)

### Requirement 2: Multi-Layered Data Validation

**User Story:** As a data engineer, I want the pipeline to enforce comprehensive validation rules beyond simple negative-count checks, so that only high-quality data enters the analytics layer.

#### Acceptance Criteria

1. WHEN inventory_count is negative, THE Validator SHALL reject the record and log the reason as "negative_inventory"
2. WHEN unit_cost is less than or equal to 0, OR retail_price is less than or equal to 0, OR unit_cost is greater than or equal to retail_price, THE Validator SHALL reject the record and log the reason as "invalid_margin"
3. WHEN sku_id does not match the format `{3_LETTERS}-{3_LETTERS}-{6_DIGITS}`, THE Validator SHALL reject the record and log the reason as "malformed_sku"
4. WHEN store_id does not match the format `{2_LETTERS}{3_LETTERS}-{3_DIGITS}`, THE Validator SHALL reject the record and log the reason as "malformed_store_id"
5. WHEN last_updated is more than 7 days before the pipeline execution date (the DAG logical execution date), THE Validator SHALL reject the record and log the reason as "stale_data"
6. WHEN inventory_count exceeds 10000 units for a single SKU at a single store, THE Validator SHALL reject the record and log the reason as "implausible_quantity"
7. WHEN any of the following required columns contains a null or empty value: sku_id, store_id, department, category, product_name, unit_cost, retail_price, inventory_count, reorder_point, warehouse_id, last_updated, THE Validator SHALL reject the record and log the reason as "missing_required_field"
8. WHEN a record fails multiple validation rules, THE Validator SHALL log all applicable rejection reasons and insert one quarantine entry per rejection reason for that record
9. IF the total number of rejected records exceeds 10% of the total records in the feed, THEN THE Pipeline SHALL raise an error and mark the DAG run as failed, indicating the rejection count and the threshold that was exceeded

### Requirement 3: Quarantine Rejected Records

**User Story:** As a data analyst, I want rejected records stored in a quarantine table with rejection reasons, so that I can investigate data quality issues without losing the problematic records.

#### Acceptance Criteria

1. WHEN a record fails one or more validation rules, THE Pipeline SHALL insert exactly one row into the Quarantine_Table with the original field values, a comma-separated list of all applicable rejection_reasons, and the pipeline_execution_date
2. THE Quarantine_Table SHALL contain columns: sku_id, store_id, department, category, product_name, inventory_count, retail_price, rejection_reason, pipeline_execution_date, raw_record where raw_record stores the original CSV row as a single unmodified text string
3. WHEN the pipeline completes, THE Pipeline SHALL log the count of quarantined records for the current execution to the Airflow task log
4. IF the pipeline is re-executed for the same pipeline_execution_date, THEN THE Pipeline SHALL replace existing quarantine records for that date rather than inserting duplicates
5. IF a quarantine insert fails due to a BigQuery error, THEN THE Pipeline SHALL fail the pipeline execution and log an error indicating the number of records that could not be quarantined

### Requirement 4: Enhanced Staging Layer

**User Story:** As a data engineer, I want the staging layer to capture the full richness of the inventory data model, so that downstream analytics can answer questions about departments, pricing, and warehouse operations.

#### Acceptance Criteria

1. WHEN validated records are loaded, THE Pipeline SHALL insert them into a staging_inventory table with all 16 columns defined in the Inventory_Feed (sku_id, store_id, department, category, product_name, size, color, unit_cost, retail_price, inventory_count, reorder_point, warehouse_id, last_received_date, last_sold_date, seasonal_flag, last_updated) plus an ingestion_timestamp column recording the UTC timestamp of when the record was inserted
2. THE Staging_Layer SHALL partition the staging_inventory table by the last_updated date column
3. WHEN loading to staging, THE Pipeline SHALL replace all partitions corresponding to the distinct last_updated dates present in the current batch of validated records rather than appending duplicate records
4. IF the staging load fails after partial insertion, THEN THE Pipeline SHALL remove any partially written records from the affected partitions and report an error indicating the failure reason and the number of records that were not loaded

### Requirement 5: Department-Level Analytics

**User Story:** As a business analyst, I want inventory summaries broken down by department and store, so that I can identify which departments are overstocked or understocked at each location.

#### Acceptance Criteria

1. WHEN the staging load completes, THE Pipeline SHALL compute department-level aggregations grouped by store_id and department, including: store_id, department, total_skus (count of distinct SKUs), total_units (sum of inventory_count), total_retail_value (sum of retail_price multiplied by inventory_count for each record), avg_inventory_per_sku (total_units divided by total_skus, rounded to 2 decimal places), below_reorder_count, and report_date set to the pipeline execution date
2. WHEN writing department-level aggregations, THE Pipeline SHALL replace any existing rows in the inventory_by_department table for the current report_date rather than appending duplicates
3. WHEN a record has inventory_count strictly less than its reorder_point, THE Pipeline SHALL count it toward the below_reorder_count for that record's store_id and department combination
4. THE Analytics_Layer SHALL store these aggregations in an inventory_by_department table

### Requirement 6: Store-Level Summary with Enrichments

**User Story:** As an operations manager, I want a store-level summary that includes financial metrics and reorder alerts, so that I can quickly assess each store's inventory health.

#### Acceptance Criteria

1. WHEN department aggregations complete, THE Pipeline SHALL compute one store-level summary row per store including: store_id, total_skus (count of distinct SKUs), total_units (sum of inventory_count), total_retail_value (sum of retail_price × inventory_count), total_cost_value (sum of unit_cost × inventory_count), departments_represented (count of distinct departments with at least one SKU), skus_below_reorder (count of SKUs where inventory_count < reorder_point), last_updated (the maximum last_updated value among the store's validated records)
2. THE Analytics_Layer SHALL store these summaries in an inventory_summary table, performing a full table replace on each pipeline execution
3. WHEN a store has more than 20% of its total SKUs below reorder point (skus_below_reorder / total_skus > 0.20), THE Pipeline SHALL log a warning to the Airflow task log indicating the store_id and the percentage rounded to one decimal place
4. IF department aggregations produce zero records for a store, THEN THE Pipeline SHALL omit that store from the inventory_summary table

### Requirement 7: Price Band Analysis

**User Story:** As a merchandising analyst, I want inventory analyzed by price band, so that I can understand the mix of budget vs premium products across stores.

#### Acceptance Criteria

1. WHEN the staging load completes, THE Pipeline SHALL classify each SKU into a Price_Band based on retail_price using exclusive lower bounds and inclusive upper bounds: Budget (greater than $0 up to $50), Mid (greater than $50 up to $200), Premium (greater than $200 up to $500), Luxury (greater than $500)
2. WHEN the staging load completes, THE Pipeline SHALL compute price band aggregations per store and price band including: store_id, price_band, sku_count (count of distinct SKUs), total_units (sum of inventory_count), total_retail_value (sum of retail_price multiplied by inventory_count), and report_date (pipeline execution date)
3. THE Analytics_Layer SHALL store these aggregations in an inventory_by_price_band table
4. WHEN price band aggregations are computed, THE Pipeline SHALL perform a full replace of the current day's data in the inventory_by_price_band table rather than appending duplicate records

### Requirement 8: Realistic Failure Scenarios

**User Story:** As a demo presenter, I want multiple realistic failure CSV files that trigger different validation failures, so that I can demonstrate various pipeline error scenarios and the alerting chain.

#### Acceptance Criteria

1. THE project SHALL include a test CSV file containing at least 10 records that all conform to the Inventory_Feed column schema, where at least 3 records trigger "invalid_margin" failures (unit_cost >= retail_price) and the remaining records are valid
2. THE project SHALL include a test CSV file containing at least 10 records that all conform to the Inventory_Feed column schema, where at least 3 records trigger "malformed_sku" failures (sku_id not matching the format `{3_LETTERS}-{3_LETTERS}-{6_DIGITS}`)
3. THE project SHALL include a test CSV file containing at least 10 records that all conform to the Inventory_Feed column schema, where at least 3 records trigger "stale_data" failures (last_updated more than 7 days before the pipeline execution date)
4. THE project SHALL include a test CSV file containing at least 20 records that all conform to the Inventory_Feed column schema, where more than 10% of records fail validation, triggering the pipeline rejection threshold failure
5. THE project SHALL include a test CSV file containing at least 20 records that all conform to the Inventory_Feed column schema, with a mix of valid and invalid records where the invalid records are fewer than 10% of total records, demonstrating that rejected records are quarantined while the pipeline completes successfully
6. WHEN any failure scenario triggers the pipeline to fail, THE Pipeline SHALL produce an error message that includes the rejection_reason category and the sku_id of each affected record
7. THE project SHALL name each failure scenario test file with a descriptive prefix indicating the failure type (e.g., `fail_invalid_margin_`, `fail_malformed_sku_`, `fail_stale_data_`, `fail_threshold_`, `fail_mixed_quarantine_`) followed by a date in the format `YYYY_MM_DD.csv`

### Requirement 9: Enhanced DAG Structure

**User Story:** As a data engineer, I want the DAG to have a more realistic structure with parallel tasks and conditional logic, so that the demo shows a production-grade pipeline architecture.

#### Acceptance Criteria

1. THE Pipeline SHALL execute the following task sequence: load_csv_from_gcs → validate_data → [load_to_staging AND load_quarantine (parallel, both must complete before proceeding)] → compute_department_metrics → compute_store_summary → compute_price_band_metrics
2. WHEN validation completes with zero rejected records, THE Pipeline SHALL mark the load_quarantine task with Airflow "skipped" status and allow downstream tasks to proceed without blocking
3. THE Pipeline SHALL use Airflow task groups organized as follows: "ingestion" group containing load_csv_from_gcs, "validation" group containing validate_data, "staging" group containing load_to_staging and load_quarantine, "analytics" group containing compute_department_metrics, compute_store_summary, and compute_price_band_metrics
4. WHEN each task completes, THE Pipeline SHALL log the task name and the number of records received as input and produced as output for that task

### Requirement 10: Realistic Sample Data

**User Story:** As a demo presenter, I want the "happy path" CSV file to contain realistic retail inventory data with varied departments, realistic product names, and plausible inventory levels, so that the demo feels authentic.

#### Acceptance Criteria

1. THE Inventory_Feed sample file SHALL contain at least 50 records with a minimum of 5 records per each of the 7 defined departments (Women's, Men's, Kids, Home, Beauty, Shoes, Accessories)
2. THE Inventory_Feed sample file SHALL include product names of 2 to 8 words that reference the product type relevant to their department (e.g., "Classic Fit Oxford Shirt" for Men's, "Hydrating Face Serum" for Beauty)
3. THE Inventory_Feed sample file SHALL include retail_price values within the following ranges per department: Beauty $8–$95, Shoes $45–$350, Accessories $15–$250, Women's/Men's/Kids $20–$400, Home $15–$500
4. THE Inventory_Feed sample file SHALL include at least 3 different warehouses as sources, with warehouse_id values following the glossary format `WH-{REGION}-{NUMBER}`
5. THE Inventory_Feed sample file SHALL include both seasonal and non-seasonal items, with at least 20% of records having seasonal_flag "Y" and at least 20% having seasonal_flag "N"
6. THE Inventory_Feed sample file SHALL include inventory_count values ranging from 0 to 500, with at least 5 records having inventory_count below their reorder_point to demonstrate reorder alerting
7. THE Inventory_Feed sample file SHALL conform to the 16-column schema defined in Requirement 1, with last_updated dates within 7 days of the file's nominal date to pass staleness validation

### Requirement 11: BigQuery Schema Updates

**User Story:** As a data engineer, I want the Terraform BigQuery configuration to define all new tables with appropriate schemas, partitioning, and clustering, so that the infrastructure supports the enhanced pipeline.

#### Acceptance Criteria

1. THE bigquery.tf file SHALL define the staging_inventory table with columns: sku_id (STRING), store_id (STRING), department (STRING), category (STRING), product_name (STRING), size (STRING), color (STRING), unit_cost (FLOAT64), retail_price (FLOAT64), inventory_count (INT64), reorder_point (INT64), warehouse_id (STRING), last_received_date (DATE), last_sold_date (DATE), seasonal_flag (STRING), last_updated (DATE), ingestion_timestamp (TIMESTAMP), partitioned by last_updated and clustered by department, store_id
2. THE bigquery.tf file SHALL define the quarantine_records table with columns: sku_id (STRING), store_id (STRING), department (STRING), category (STRING), product_name (STRING), inventory_count (INT64), retail_price (FLOAT64), rejection_reason (STRING), pipeline_execution_date (DATE), raw_record (STRING), partitioned by pipeline_execution_date
3. THE bigquery.tf file SHALL define the inventory_by_department table with columns: store_id (STRING), department (STRING), total_skus (INT64), total_units (INT64), total_retail_value (FLOAT64), avg_inventory_per_sku (FLOAT64), below_reorder_count (INT64), report_date (DATE), partitioned by report_date and clustered by department
4. THE bigquery.tf file SHALL define the inventory_by_price_band table with columns: store_id (STRING), price_band (STRING), sku_count (INT64), total_units (INT64), total_retail_value (FLOAT64), report_date (DATE), partitioned by report_date
5. THE bigquery.tf file SHALL define the inventory_summary table with columns: store_id (STRING), total_skus (INT64), total_units (INT64), total_retail_value (FLOAT64), total_cost_value (FLOAT64), departments_represented (INT64), skus_below_reorder (INT64), last_updated (DATE)

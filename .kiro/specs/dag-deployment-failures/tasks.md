# Implementation Plan: DAG/Code Deployment Failure Scenarios

## Overview

Four DAG files simulating real Macy's deployment failures, plus minimal infrastructure updates (alert filter + Cloud Function) to support parse-time error detection. Each DAG file is independent and can be uploaded individually to trigger the corresponding failure scenario.

## Tasks

- [x] 1. Update infrastructure for deployment failure detection
  - [x] 1.1 Add Broken DAG and module error patterns to the alert policy filter in jira_alerting.tf
    - Add `textPayload=~"Broken DAG" OR` to the condition_matched_log filter
    - Add `textPayload=~"No module named" OR` to the condition_matched_log filter
    - Place these lines after the existing `textPayload=~"task_id=.*state=failed"` line
    - _Requirements: 5.1, 5.5_

  - [x] 1.2 Add new DAG names to the Cloud Function known_dag list in cloud_function/main.py
    - Add `"pricing_margin_calculator"`, `"store_replenishment_optimizer"`, `"inventory_allocation_engine"`, `"fulfillment_routing_pipeline"` to the `known_dag` list
    - _Requirements: 5.2, 5.3, 5.4_

- [ ] 2. Create TC-1: Bad DAG Deploy — Syntax Error
  - [ ] 2.1 Create pricing_margin_calculator_dag.py with a realistic syntax error
    - Write a DAG named `pricing_margin_calculator` for computing pricing margins across departments
    - Include valid imports (airflow, datetime, logging), default_args, and 2-3 task functions
    - Introduce a syntax error in the third function: an incomplete dict literal (missing closing brace) that simulates a truncated file upload or merge conflict
    - Use `schedule_interval=None`, tags `["retail", "pricing", "margins"]`
    - Ensure no words like "demo", "test", "simulation", "fake", "intentional" appear anywhere
    - The file should look ~60 lines, with the error around line 50-55
    - _Requirements: 1.1, 1.2, 1.4, 1.5, 1.6, 6.1_

- [ ] 3. Create TC-2: Missing Module — Undeployed Dependency
  - [ ] 3.1 Create store_replenishment_optimizer_dag.py that imports a non-existent module
    - Write a DAG named `store_replenishment_optimizer` for optimizing store restocking
    - Add top-level import: `from mfm_inventory_utils import DemandForecaster, ReplenishmentCalculator, StoreCapacityManager`
    - Write complete task functions that USE the imported classes (DemandForecaster for demand prediction, ReplenishmentCalculator for order quantities, StoreCapacityManager for capacity checks)
    - Use `schedule_interval=None`, tags `["retail", "supply-chain", "replenishment"]`
    - Ensure no words like "demo", "test", "simulation", "fake", "intentional" appear anywhere
    - The file should be ~80 lines of realistic, complete code
    - _Requirements: 2.1, 2.2, 2.4, 2.5, 2.6, 6.2_

- [ ] 4. Create TC-3: Breaking Change in Shared Library
  - [ ] 4.1 Create inventory_allocation_engine_dag.py with a function signature mismatch
    - Write a DAG named `inventory_allocation_engine` for cross-store inventory allocation
    - Define a helper function `calculate_allocation_priority` at the top with the NEW signature: `(sku_id, store_id, current_stock, buffer_multiplier, demand_velocity)` — include a docstring mentioning "Updated in v2.4: renamed safety_stock_factor to buffer_multiplier"
    - In the task function, call `calculate_allocation_priority()` with the OLD keyword argument `safety_stock_factor=1.5` (this triggers TypeError at runtime)
    - Use `schedule_interval=None`, tags `["retail", "inventory", "allocation"]`
    - Include realistic allocation logic context (loading store data, iterating SKUs, computing priorities)
    - Ensure no words like "demo", "test", "simulation", "fake", "intentional" appear anywhere
    - The file should be ~90 lines; DAG must parse successfully
    - _Requirements: 3.1, 3.2, 3.3, 3.5, 3.6, 3.7, 6.3_

- [ ] 5. Create TC-4: Version Incompatibility — Deprecated Airflow API
  - [ ] 5.1 Create fulfillment_routing_pipeline_dag.py with a deprecated Airflow import
    - Write a DAG named `fulfillment_routing_pipeline` for routing fulfillment orders to warehouses
    - Use standard Airflow 2.x imports at the top level (PythonOperator, DAG, etc.) so the file parses
    - Inside the main task function, include a deferred import: `from airflow.contrib.operators.bigquery_operator import BigQueryOperator`
    - Add logging before the import: `logger.info("Loading legacy routing configuration from pre-upgrade codebase...")`
    - The deferred import triggers `ModuleNotFoundError: No module named 'airflow.contrib.operators.bigquery_operator'` at runtime
    - Use `schedule_interval=None`, tags `["retail", "fulfillment", "routing"]`
    - Include realistic fulfillment routing context (warehouse selection, distance calculation, capacity checks)
    - Ensure no words like "demo", "test", "simulation", "fake", "intentional" appear anywhere
    - The file should be ~85 lines; DAG must parse successfully
    - _Requirements: 4.1, 4.2, 4.3, 4.5, 4.6, 4.7, 6.4_

- [ ] 6. Update DEMO_RUNBOOK.md with deployment failure scenarios
  - [ ] 6.1 Add deployment failure scenarios section to DEMO_RUNBOOK.md
    - Add a new section "Deployment Failure Scenarios (TC-5 through TC-8)" below existing scenarios
    - Include trigger commands, cleanup commands, expected Jira titles, and timing for each scenario
    - Include the "Between Scenarios" instructions (close alert, wait 5 min)
    - Reference the test case sheet for full details
    - _Requirements: 1.3, 2.3, 3.4, 4.4_

- [ ] 7. Verify all DAG files
  - [ ] 7.1 Verify TC-1 file has valid Python except for the intentional syntax error
    - Run `python -c "import ast; ast.parse(open('pricing_margin_calculator_dag.py').read())"` — confirm it raises SyntaxError
    - Verify the error message includes a line number and "invalid syntax"

  - [ ] 7.2 Verify TC-2 file has valid Python syntax but fails on import
    - Run `python -c "import ast; ast.parse(open('store_replenishment_optimizer_dag.py').read())"` — confirm it parses (no SyntaxError)
    - Run `python store_replenishment_optimizer_dag.py` — confirm ModuleNotFoundError for 'mfm_inventory_utils'

  - [ ] 7.3 Verify TC-3 file parses and registers but fails at runtime
    - Run `python -c "import ast; ast.parse(open('inventory_allocation_engine_dag.py').read())"` — confirm it parses
    - Run `python -c "exec(open('inventory_allocation_engine_dag.py').read())"` — confirm DAG object is created (no parse error)
    - Verify the TypeError would occur by calling the task function directly

  - [ ] 7.4 Verify TC-4 file parses and registers but fails at runtime
    - Run `python -c "import ast; ast.parse(open('fulfillment_routing_pipeline_dag.py').read())"` — confirm it parses
    - Verify the deferred import inside the task function references `airflow.contrib.operators.bigquery_operator`

## Notes

- No property-based tests needed — these are static DAG files, not logic to validate
- Verification is done by checking parse behavior (SyntaxError vs clean parse) and runtime behavior
- The infrastructure changes (task 1) should be applied with `terraform apply` before uploading DAG files
- After `terraform apply`, redeploy the Cloud Function: the function code change (task 1.2) requires redeployment

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2"] },
    { "id": 1, "tasks": ["2.1", "3.1", "4.1", "5.1"] },
    { "id": 2, "tasks": ["6.1"] },
    { "id": 3, "tasks": ["7.1", "7.2", "7.3", "7.4"] }
  ]
}
```

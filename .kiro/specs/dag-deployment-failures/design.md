# Design Document: DAG/Code Deployment Failure Scenarios

## Overview

Four DAG files that simulate real Macy's deployment failures when uploaded to the Cloud Composer DAGs bucket. Two produce parse-time "Broken DAG" errors (TC-1, TC-2) and two produce runtime task failures (TC-3, TC-4). All trigger the existing alerting chain and produce distinct error patterns for unique OpsRabbit RCA outputs.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│ Composer DAGs Bucket (GCS)                                       │
│                                                                   │
│  pricing_margin_calculator_dag.py ──────┐                        │
│  store_replenishment_optimizer_dag.py ───┤  Parse-time failures  │
│                                          ▼                        │
│                              DAG Processor → Broken DAG log       │
│                                                                   │
│  inventory_allocation_engine_dag.py ────┐                        │
│  fulfillment_routing_pipeline_dag.py ───┤  Runtime failures      │
│                                          ▼                        │
│                         Task executes → raises exception → log    │
└──────────────────────────────────────────┬────────────────────────┘
                                           │
                                           ▼
                              Cloud Monitoring Alert Policy
                              (log-based, severity>=ERROR)
                                           │
                                           ▼
                              Pub/Sub → Cloud Function → Jira → OpsRabbit RCA
```

## Design Decisions

### 1. Parse-time vs Runtime Failures

TC-1 and TC-2 are designed as genuine parse-time failures. When the DAG Processor encounters a `SyntaxError` or `ModuleNotFoundError`, it logs to Cloud Logging at severity ERROR with text containing "Broken DAG". This requires adding `textPayload=~"Broken DAG"` to the existing alert filter — a single line addition to `jira_alerting.tf`.

**Rationale:** Real deployment failures at Macy's manifest as Broken DAGs, not runtime errors. Simulating them as runtime errors would produce misleading RCA and wouldn't replicate the actual incident pattern.

### 2. Alert Filter Update (Minimal)

The existing alert filter in `jira_alerting.tf` needs two additional patterns:

```
textPayload=~"Broken DAG" OR
textPayload=~"No module named"
```

This is the only infrastructure change required. All other components (Pub/Sub, Cloud Function, Jira integration, OpsRabbit) remain unchanged.

### 3. DAG Naming Convention

DAG names use realistic Macy's retail business terminology:
- `pricing_margin_calculator` — pricing team DAG
- `store_replenishment_optimizer` — supply chain team DAG  
- `inventory_allocation_engine` — allocation team DAG
- `fulfillment_routing_pipeline` — fulfillment team DAG

No "demo", "test", or "simulation" language anywhere in the files.

### 4. Error Distinctiveness Strategy

Each scenario produces a fundamentally different error class so OpsRabbit generates unique RCA:

| TC | Error Class | Diagnostic Signal | RCA Direction |
|----|-------------|-------------------|---------------|
| TC-1 | `SyntaxError` | Line number, `invalid syntax`, incomplete dict | Corrupted file / bad merge |
| TC-2 | `ModuleNotFoundError` | Package name `mfm_inventory_utils` | Missing dependency package |
| TC-3 | `TypeError` | Function signature mismatch, keyword argument | Breaking API change in shared lib |
| TC-4 | `ModuleNotFoundError` (runtime) | `airflow.contrib` path removed | Version incompatibility after upgrade |

---

## TC-1: Bad DAG Deploy — Syntax Error

### File: `pricing_margin_calculator_dag.py`

**Approach:** A realistic DAG for calculating pricing margins across departments. The file contains a syntax error that looks like a truncated deployment — a dict literal is left incomplete (missing closing brace and trailing code). This mimics what happens when a CI/CD pipeline truncates a file or a merge conflict leaves broken syntax.

**Error Trigger:** The DAG Processor attempts to compile the Python file and encounters:
```
SyntaxError: invalid syntax (pricing_margin_calculator_dag.py, line NN)
```

**Log Entry Pattern (Composer Cloud Logging):**
```
Broken DAG [/home/airflow/gcs/dags/pricing_margin_calculator_dag.py] Traceback:
  File "/home/airflow/gcs/dags/pricing_margin_calculator_dag.py", line NN
    ...
SyntaxError: invalid syntax
```

**Design Details:**
- File starts normally with imports, default_args, and two valid task functions
- Error occurs inside the third function where a department config dict is incomplete
- The truncation point looks natural — as if bytes were lost during GCS upload
- Total file: ~60 lines, error around line 50-55

---

## TC-2: Missing Module — Undeployed Dependency

### File: `store_replenishment_optimizer_dag.py`

**Approach:** A DAG for optimizing store replenishment using demand forecasting. It imports `mfm_inventory_utils` (a plausible internal Macy's Fulfillment Module package) at the top level. Since this module doesn't exist in the Composer environment, the DAG Processor fails at import time.

**Error Trigger:** The DAG Processor attempts to import the file and encounters:
```
ModuleNotFoundError: No module named 'mfm_inventory_utils'
```

**Log Entry Pattern (Composer Cloud Logging):**
```
Broken DAG [/home/airflow/gcs/dags/store_replenishment_optimizer_dag.py] Traceback:
  File "/home/airflow/gcs/dags/store_replenishment_optimizer_dag.py", line 8, in <module>
    from mfm_inventory_utils import DemandForecaster, ReplenishmentCalculator, StoreCapacityManager
ModuleNotFoundError: No module named 'mfm_inventory_utils'
```

**Design Details:**
- File uses a realistic import statement referencing specific classes from the fake module
- The rest of the file is complete, well-structured code that would work if the module existed
- Uses the imported classes in task functions (DemandForecaster, ReplenishmentCalculator, StoreCapacityManager)
- Total file: ~80 lines
- The module name `mfm_inventory_utils` suggests it's an internal Macy's Fulfillment Module package

---

## TC-3: Breaking Change in Shared Library

### File: `inventory_allocation_engine_dag.py`

**Approach:** A DAG for inventory allocation across stores. It imports a local helper module that simulates a shared library (`mfm_allocation_core`). The DAG calls a function using the OLD parameter name (`safety_stock_factor`), but the "updated" module now expects `buffer_multiplier`. This produces a TypeError at runtime.

**Implementation Strategy:**
- The DAG file defines a small inline module (using a class or imported sub-file) that represents the "updated" shared library
- The task function calls the module function with the old keyword argument
- This fails at runtime with: `TypeError: calculate_allocation_priority() got an unexpected keyword argument 'safety_stock_factor'`

**Error Trigger:** When the task runs, it calls:
```python
result = calculate_allocation_priority(
    sku_id=sku,
    store_id=store,
    current_stock=150,
    safety_stock_factor=1.5  # OLD parameter name — renamed to buffer_multiplier in v2.4
)
```

The function now has signature:
```python
def calculate_allocation_priority(sku_id, store_id, current_stock, buffer_multiplier, demand_velocity):
```

**Log Entry Pattern (Composer Cloud Logging):**
```
Task failed with exception
...
TypeError: calculate_allocation_priority() got an unexpected keyword argument 'safety_stock_factor'
```

**Design Details:**
- The "updated" module is defined at the top of the DAG file as a helper function (no separate file needed)
- The function has a realistic signature for allocation priority scoring
- Error message clearly shows the parameter name mismatch
- DAG parses fine because the function exists — error only occurs at call time
- Includes a comment block at the top of the helper function referencing "v2.4 migration" to give OpsRabbit context
- Total file: ~90 lines

---

## TC-4: Version Incompatibility — Deprecated Airflow API

### File: `fulfillment_routing_pipeline_dag.py`

**Approach:** A DAG for routing fulfillment orders to optimal warehouses. It parses and registers fine because the deprecated import (`airflow.contrib.operators.bigquery_operator`) is done inside a task function (deferred import). When the task runs, the import fails because `airflow.contrib` was removed in Airflow 2.x.

**Implementation Strategy:**
- Top-level imports use standard Airflow 2.x APIs so the DAG parses
- Inside the task function, a deferred import attempts to load from the deprecated path:
  ```python
  from airflow.contrib.operators.bigquery_operator import BigQueryOperator
  ```
- This fails at runtime with a ModuleNotFoundError referencing the deprecated path

**Error Trigger:** When the task executes:
```python
def route_fulfillment_orders(**context):
    # Legacy import from pre-upgrade codebase (Airflow 1.10 / Composer 1)
    from airflow.contrib.operators.bigquery_operator import BigQueryOperator
    ...
```

**Log Entry Pattern (Composer Cloud Logging):**
```
Task failed with exception
...
ModuleNotFoundError: No module named 'airflow.contrib.operators.bigquery_operator'
```

**Design Details:**
- DAG uses standard Airflow 2.x imports at the top level (so it parses)
- The deprecated import is inside the task function body (deferred to runtime)
- Error message references the old `airflow.contrib` path, making it clear this is a version issue
- Includes logging that references "loading legacy routing configuration" to give OpsRabbit context about why the old import exists
- Total file: ~85 lines

---

## Alert Filter Update

### Current Filter (relevant excerpt):
```
(textPayload=~"Task failed with exception" OR
 textPayload=~"Marking task as FAILED" OR
 textPayload=~"Task exited with return code" OR
 textPayload=~"Task timed out" OR
 textPayload=~"OOMKilled" OR
 textPayload=~"Duplicate data detected" OR
 textPayload=~"Log file does not exist" OR
 textPayload=~"task_id=.*state=failed")
```

### Required Addition:
```
textPayload=~"Broken DAG" OR
textPayload=~"No module named"
```

**Rationale:** 
- `"Broken DAG"` captures TC-1 (SyntaxError) and TC-2 (ModuleNotFoundError at parse time)
- `"No module named"` also captures TC-4 (runtime ModuleNotFoundError for deprecated path)
- TC-3 already matches `"Task failed with exception"` (TypeError at runtime)

---

## Cloud Function Compatibility

The existing Cloud Function in `cloud_function/main.py` extracts DAG names by:
1. Checking the alert payload for known DAG names
2. Querying Cloud Logging for recent `"Marking task as FAILED. dag_id="` entries

For TC-3 and TC-4 (runtime failures), method 2 will find the dag_id in the task failure log.

For TC-1 and TC-2 (parse-time failures), the Broken DAG log contains the file path (e.g., `/home/airflow/gcs/dags/pricing_margin_calculator_dag.py`). The Cloud Function's known DAG list needs to be updated to include the new DAG names.

### Cloud Function Update:
Add new DAG names to the `known_dag` list:
```python
for known_dag in ["inventory_store_reconciliation", "inventory_cross_store_analysis",
                  "daily_inventory_pipeline", "inventory_batch_processor",
                  "pricing_margin_calculator", "store_replenishment_optimizer",
                  "inventory_allocation_engine", "fulfillment_routing_pipeline"]:
```

---

## File Summary

| File | Lines | Parse Result | Runtime Result |
|------|-------|-------------|----------------|
| `pricing_margin_calculator_dag.py` | ~60 | SyntaxError (Broken DAG) | N/A |
| `store_replenishment_optimizer_dag.py` | ~80 | ModuleNotFoundError (Broken DAG) | N/A |
| `inventory_allocation_engine_dag.py` | ~90 | Success | TypeError on trigger |
| `fulfillment_routing_pipeline_dag.py` | ~85 | Success | ModuleNotFoundError on trigger |

---

## Deployment Procedure

```bash
# Get DAG bucket path
DAG_PATH=$(gcloud composer environments describe inventory-pipeline \
  --location=us-central1 --format="value(config.dagGcsPrefix)")

# TC-1: Upload broken syntax DAG
gsutil cp pricing_margin_calculator_dag.py $DAG_PATH/

# TC-2: Upload missing module DAG
gsutil cp store_replenishment_optimizer_dag.py $DAG_PATH/

# TC-3: Upload + trigger breaking change DAG
gsutil cp inventory_allocation_engine_dag.py $DAG_PATH/
# Wait 2 min for registration
gcloud composer environments run inventory-pipeline --location us-central1 dags trigger -- inventory_allocation_engine

# TC-4: Upload + trigger deprecated API DAG
gsutil cp fulfillment_routing_pipeline_dag.py $DAG_PATH/
# Wait 2 min for registration
gcloud composer environments run inventory-pipeline --location us-central1 dags trigger -- fulfillment_routing_pipeline
```

## Cleanup

```bash
gsutil rm $DAG_PATH/pricing_margin_calculator_dag.py
gsutil rm $DAG_PATH/store_replenishment_optimizer_dag.py
gsutil rm $DAG_PATH/inventory_allocation_engine_dag.py
gsutil rm $DAG_PATH/fulfillment_routing_pipeline_dag.py
```

# Demo Runbook — Macy's Inventory Pipeline Failure → AI RCA

**Goal:** Airflow pipeline fails → Cloud Monitoring alert → Cloud Function → Jira ticket created → OpsRabbit AI agent investigates → posts RCA findings as Jira comment

---

## 4 Demo Scenarios

| TC | Scenario | DAG Name | What Agent Should Find |
|----|----------|----------|----------------------|
| TC-1 | **Job Stuck** (frozen, no progress) | `inventory_store_reconciliation` | Task stuck — deadlock or hanging database connection |
| TC-2 | **Long-Running** (slow progress, timeout) | `inventory_cross_store_analysis` | Unoptimized query scanning 2.4TB, too slow |
| TC-3 | **Duplicate Data** | `daily_inventory_pipeline` | Same SKU+Store appears multiple times |
| TC-4 | **Log File Not Found** (worker evicted) | `inventory_batch_processor` | Worker OOM-killed, log lost |

---

## One-Time Setup

```bash
# 1. Deploy infrastructure (alert policy, Cloud Function, BigQuery tables)
terraform apply

# 2. Upload all DAGs and validators to Composer
DAG_PATH=$(gcloud composer environments describe inventory-pipeline --location=us-central1 --format="value(config.dagGcsPrefix)")
gsutil cp inventory_pipeline_dag.py $DAG_PATH/
gsutil cp validators.py $DAG_PATH/
gsutil cp stuck_job_dag.py $DAG_PATH/
gsutil cp long_running_dag.py $DAG_PATH/
gsutil cp log_not_found_dag.py $DAG_PATH/

# 3. Wait 2 minutes, verify all DAGs are registered
gcloud composer environments run inventory-pipeline --location us-central1 dags list
```

---

## How to Run Each Scenario

### TC-1: Job Stuck

```bash
gcloud composer environments run inventory-pipeline --location us-central1 dags trigger -- inventory_store_reconciliation
```

- Task `reconcile_store_counts` will freeze for 2 minutes (no output)
- After 2 min → task killed → alert fires
- Wait 3-5 min → Jira ticket created → OpsRabbit investigates

### TC-2: Long-Running

```bash
gcloud composer environments run inventory-pipeline --location us-central1 dags trigger -- inventory_cross_store_analysis
```

- Task `compute_cross_store_analysis` will show slow progress every 20 seconds
- After 2 min → task killed → alert fires
- Wait 3-5 min → Jira ticket created → OpsRabbit investigates

### TC-3: Duplicate Data

```bash
# Step 1: Upload the duplicate data CSV (renamed to today's date)
gsutil cp fail_duplicate_data_2026_03_10.csv gs://aaic-opsrabbit-demo-retail-inventory-001/inventory/inventory_$(date +%Y_%m_%d).csv

# Step 2: Trigger the pipeline
gcloud composer environments run inventory-pipeline --location us-central1 dags trigger -- daily_inventory_pipeline
```

- Task `validate_data` fails immediately (detects duplicate SKU+Store)
- Alert fires → Jira ticket created → OpsRabbit investigates

### TC-4: Log File Not Found

```bash
gcloud composer environments run inventory-pipeline --location us-central1 dags trigger -- inventory_batch_processor
```

- Task `process_inventory_updates` crashes after ~10 seconds (OOM kill simulation)
- Error includes "Log file does not exist" message
- Alert fires → Jira ticket created → OpsRabbit investigates

---

## After Triggering Any Scenario

```bash
# Wait 3-5 minutes, then check Cloud Function logs
gcloud functions logs read jira-ticket-creator --region=us-central1 --gen2 --limit=5
```

Look for:
- `Jira ticket created: OR-XX` ✅
- `OpsRabbit notified: 202` ✅

Then check:
- **Jira:** https://opsrabbit.atlassian.net/projects/OR → new ticket
- **OpsRabbit:** https://sre.7targets.ai/ → new RCA chat

---

## Between Scenarios (Important!)

Before triggering the next scenario:

1. **Close the open alert incident:** GCP Console → Monitoring → Alerting → click the incident → "Acknowledge" or close it
2. **Wait 5 minutes** (alert rate limit prevents duplicate firing)
3. Then trigger the next scenario

---

## Expected Jira Ticket Titles

| TC | Jira Title |
|----|------------|
| TC-1 | `[Composer Alert] Composer DAG Task Failure for inventory_store_reconciliation` |
| TC-2 | `[Composer Alert] Composer DAG Task Failure for inventory_cross_store_analysis` |
| TC-3 | `[Composer Alert] Composer DAG Task Failure for daily_inventory_pipeline` |
| TC-4 | `[Composer Alert] Composer DAG Task Failure for inventory_batch_processor` |

---

## Timing Per Scenario

| Step | TC-1 & TC-2 | TC-3 & TC-4 |
|------|-------------|-------------|
| Task runs | 2 min (timeout) | ~10 sec (fast fail) |
| Alert fires | 1-5 min | 1-5 min |
| Jira ticket created | seconds | seconds |
| OpsRabbit RCA | 30-60 sec | 30-60 sec |
| **Total** | **~5-8 min** | **~3-7 min** |

# TC-1: Job Stuck
gcloud composer environments run inventory-pipeline --location us-central1 dags trigger -- inventory_store_reconciliation

# TC-2: Long-Running
gcloud composer environments run inventory-pipeline --location us-central1 dags trigger -- inventory_cross_store_analysis

# TC-3: Duplicate Data
gsutil cp fail_duplicate_data_2026_03_10.csv gs://aaic-opsrabbit-demo-retail-inventory-001/inventory/inventory_$(date +%Y_%m_%d).csv
gcloud composer environments run inventory-pipeline --location us-central1 dags trigger -- daily_inventory_pipeline

# TC-4: Log File Not Found
gcloud composer environments run inventory-pipeline --location us-central1 dags trigger -- inventory_batch_processor

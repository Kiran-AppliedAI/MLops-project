# GCP Composer Pipeline RCA Investigation

## Skill Description
Investigate Cloud Composer (Airflow) pipeline failures by querying GCP Cloud Logging. Use this skill when a Jira ticket is created due to a Composer pipeline failure to determine the root cause.

## Environment Details
- Project: `aaic-opsrabbit-demo`
- Composer Environment: `inventory-pipeline`
- Region: `us-central1`
- DAG: `daily_inventory_pipeline`
- BigQuery Dataset: `retail_analytics`
- GCS Bucket: `aaic-opsrabbit-demo-retail-inventory-001`

## IMPORTANT RULES FOR COMMANDS
- Run ALL commands exactly as written below using shell/bash execution
- Do NOT add `--location` flag to `gcloud logging read` commands
- Do NOT add `--freshness` flag to `gcloud logging read` commands
- Do NOT modify the filter strings
- Use `--locations` (plural with s) for `gcloud composer` commands
- Use `--project` (not `--location`) for `gcloud logging read` commands
- Use `timestamp>=` for time filtering in log queries

---

## Step 1: Verify GCP Authentication

```bash
gcloud auth list
```

If no active account shown, activate:

```bash
gcloud auth activate-service-account --key-file=/app/opsrabbit-agent-key.json
gcloud config set project aaic-opsrabbit-demo
```

---

## Step 2: Check Composer Environment Status

```bash
gcloud composer environments list --locations=us-central1 --project=aaic-opsrabbit-demo
```

---

## Step 3: Fetch All Recent Airflow Error Logs

```bash
gcloud logging read 'logName=~"projects/aaic-opsrabbit-demo/logs/airflow" AND severity>=ERROR' --project=aaic-opsrabbit-demo --limit=20 --format=json
```

If this returns empty, try without severity filter to get any recent logs:

```bash
gcloud logging read 'logName=~"projects/aaic-opsrabbit-demo/logs/airflow"' --project=aaic-opsrabbit-demo --limit=10 --format=json
```

---

## Step 4: Fetch Airflow Worker Logs (Task Failures and Stack Traces)

```bash
gcloud logging read 'logName="projects/aaic-opsrabbit-demo/logs/airflow-worker"' --project=aaic-opsrabbit-demo --limit=20 --format=json
```

---

## Step 5: Fetch Logs for the Inventory Pipeline DAG

```bash
gcloud logging read 'logName=~"projects/aaic-opsrabbit-demo/logs/airflow" AND textPayload=~"daily_inventory_pipeline"' --project=aaic-opsrabbit-demo --limit=20 --format=json
```

---

## Step 6: Fetch Airflow Scheduler Logs

```bash
gcloud logging read 'logName="projects/aaic-opsrabbit-demo/logs/airflow-scheduler"' --project=aaic-opsrabbit-demo --limit=10 --format=json
```

---

## Step 7: Search for Specific Failure Patterns

```bash
gcloud logging read 'logName=~"projects/aaic-opsrabbit-demo/logs/airflow" AND (textPayload=~"Task failed with exception" OR textPayload=~"Marking task as FAILED" OR textPayload=~"Traceback" OR textPayload=~"Error" OR textPayload=~"Exception")' --project=aaic-opsrabbit-demo --limit=20 --format=json
```

---

## Step 8: Check Cloud Function Logs (Jira Ticket Creator)

```bash
gcloud logging read 'resource.type="cloud_run_revision" AND resource.labels.service_name="jira-ticket-creator"' --project=aaic-opsrabbit-demo --limit=10 --format=json
```

---

## Step 9: Check Input Data in GCS

List CSV files:

```bash
gcloud storage ls gs://aaic-opsrabbit-demo-retail-inventory-001/inventory/
```

Read the most recent CSV (replace DATE with the date from the error logs in YYYY_MM_DD format):

```bash
gcloud storage cat gs://aaic-opsrabbit-demo-retail-inventory-001/inventory/inventory_DATE.csv
```

Look for: negative inventory counts, missing fields, empty rows, malformed data.

---

## Step 10: Check BigQuery Data

```bash
bq query --use_legacy_sql=false --project_id=aaic-opsrabbit-demo "SELECT * FROM aaic-opsrabbit-demo.retail_analytics.staging_inventory ORDER BY last_updated DESC LIMIT 10"
```

```bash
bq query --use_legacy_sql=false --project_id=aaic-opsrabbit-demo "SELECT * FROM aaic-opsrabbit-demo.retail_analytics.inventory_summary"
```

---

## Common Failure Patterns

| Log Pattern | Likely Cause | Fix |
|-------------|-------------|-----|
| `negative inventory count` | Bad CSV data with negative values | Re-upload corrected CSV |
| `No data found in gs://` | Missing CSV for execution date | Upload CSV named `inventory_YYYY_MM_DD.csv` |
| `404 No such object` | CSV file missing for that date | Upload the correct dated CSV file |
| `Task failed with exception` | Python error in DAG task | Check stack trace in worker logs |
| `Marking task as FAILED` | Task exceeded retry limit | Check retry config and root error |
| `Permission denied` | Service account missing roles | Check IAM bindings |
| `Traceback` | Unhandled Python exception | Read full stack trace |

---

## Step 11: Post RCA Summary as Jira Comment

IMPORTANT: After completing all investigation steps above, you MUST post a final concluding comment on the Jira ticket with your RCA findings. Use the Jira comment tool to add a comment to the ticket that triggered this investigation.

The comment MUST follow this format:

```
🔍 RCA Investigation Complete

Root Cause: [Brief description of what caused the failure]
Failed Task: [Task name, e.g., validate_data, load_csv_from_gcs]
Error Message: [Key error from logs]
Evidence: [Log timestamp and relevant log entry]
Impact: [What was affected — data not loaded, summary not updated, etc.]
Recommendation: [What to fix — re-upload CSV, fix DAG code, fix permissions, etc.]
Status: Investigation complete
```

This final comment is critical — it provides the team with actionable findings directly on the Jira ticket.

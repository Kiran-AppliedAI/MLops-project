# OpsRabbit Inventory MLOps Demo

This repository is an MLOps / AI-ops demo for retail inventory processing and operational RCA. It combines infrastructure-as-code, workflow orchestration, data validation, and automated incident response.

## Why this is MLOps

- CI/CD & IaC: Terraform defines the GCP infra (Composer, GCS, BigQuery, Cloud Functions, IAM, Secrets).
- Orchestration: Airflow DAGs implement data ingestion, validation, staging, and summary reports.
- Data quality & governance: `validators.py` enforces business rules and quarantine logic; unit tests validate behavior.
- Automation & AI-ops: Monitoring alerts trigger a Cloud Function that creates Jira tickets and notifies an AI RCA agent (OpsRabbit).

## Key components

- `composer.tf`, `main.tf`, `storage.tf`, `bigquery.tf`, `iam.tf`, `cloud_function.tf` — Terraform resources
- `inventory_pipeline_dag.py`, `validators.py`, `stuck_job_dag.py`, `long_running_dag.py` — Airflow DAGs and validators
- `cloud_function/main.py` — Jira-ticket creator + OpsRabbit notifier
- `tests/unit/` — unit tests for validators and CSV parsing
- `DEMO_RUNBOOK.md`, `GCP_SETUP_GUIDE.md` — run and setup instructions for demos

## Recommended repo name

`opsrabbit-inventory-mlops-demo` (recommended). You can also use `macys-airflow-demo-terraform` to match historical naming.

## How to present this repo as MLOps

- Put the recommended repo name in the Git remote and project metadata.
- Use the `README.md` as the project entry point in GitHub and add topics: `mlops`, `dataops`, `airflow`, `terraform`, `gcp`, `ai-ops`.
- Keep `DEMO_RUNBOOK.md` and `GCP_SETUP_GUIDE.md` visible in the repo root to show operational playbooks.

## Next steps (optional)

- Update `REPO_SUMMARY.md` to call out MLOps explicitly (I can do this).









One-line project title

OpsRabbit Inventory MLOps Demo — End-to-end MLOps & AI-ops pipeline for retail inventory (GCP, Terraform, Airflow, BigQuery)
Resume bullets (pick 3–4)

Architected and implemented an end-to-end MLOps demo: provisioned GCP infra with Terraform (Cloud Composer, GCS, BigQuery, Cloud Functions, Pub/Sub, Secret Manager) and deployed Airflow DAGs to automate ingestion, validation, staging, and reporting.
Built robust data-quality controls and governance: authored validation library and unit tests to detect malformed SKUs, invalid margins, duplicates, stale records, and enforce a 10% rejection threshold; quarantined bad rows before BigQuery ingestion.
Automated incident response and RCA: implemented a Cloud Function (main.py) triggered by Monitoring alerts to auto-create Jira tickets and notify an AI RCA agent, shortening mean time to investigation in demo scenarios.
Ensured reproducible, secure deployment: used Terraform IaC, service accounts and Secret Manager for credentials, and documented runbook and setup guides for repeatable demos and handoffs.
Tech stack (single line)

GCP (Composer, BigQuery, Cloud Functions, GCS, Pub/Sub, Secret Manager), Terraform, Airflow, Python, pytest, Jira, OpsRabbit (AI RCA)
LinkedIn / short summary (one sentence)

Developed an MLOps & AI-ops demo that automates data validation, secure cloud deployment, and AI-assisted incident RCA for retail inventory pipelines.
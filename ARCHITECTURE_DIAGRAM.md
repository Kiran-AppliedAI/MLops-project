# Architecture Diagram — OpsRabbit Inventory MLOps Demo

Simple architecture diagram (Mermaid) showing what was implemented and the main services/tools used.

```mermaid
flowchart LR
  subgraph IaC
    TF[Terraform]
  end

  subgraph GCP
    GCS[Cloud Storage\n(inventory CSV)]
    Composer[Cloud Composer\n(Airflow DAGs)]
    BQ[BigQuery\n(staging, summary, quarantine)]
    Monitoring[Cloud Monitoring]
    PubSub[Pub/Sub]
    CloudFn[Cloud Function\n(Jira ticket creator)]
    Secret[Secret Manager]
    IAM[Service Accounts & IAM]
  end

  subgraph External
    Jira[Jira]
    OpsRabbit[OpsRabbit AI RCA]
  end

  TF -->|provisions| Composer
  TF -->|provisions| GCS
  TF -->|provisions| BQ
  TF -->|provisions| CloudFn
  TF -->|provisions| PubSub
  TF -->|provisions| Secret
  TF -->|provisions| IAM

  GCS -->|CSV files| Composer
  Composer -->|executes DAGs / calls| Validators[validators.py]
  Validators -->|valid / quarantine| BQ
  Composer -->|task failures| Monitoring
  Monitoring -->|alert (to Pub/Sub)| PubSub
  PubSub --> CloudFn
  CloudFn -->|creates ticket| Jira
  CloudFn -->|notifies| OpsRabbit
  CloudFn -->|reads token| Secret
  IAM --> Composer
  IAM --> CloudFn
  BQ -->|reports / analytics| OpsDashboard[Reporting / BI]

  style TF fill:#f9f,stroke:#333,stroke-width:1px
  style GCP fill:#efe,stroke:#333,stroke-width:1px
  style External fill:#eef,stroke:#333,stroke-width:1px
```

Files / components mapping

- Infrastructure as Code: `main.tf`, `composer.tf`, `storage.tf`, `bigquery.tf`, `iam.tf`, `cloud_function.tf`
- Airflow & validation: `inventory_pipeline_dag.py`, `validators.py`, `stuck_job_dag.py`, `long_running_dag.py`
- Cloud Function: `cloud_function/main.py` (Jira + OpsRabbit notifier)
- Tests: `tests/unit/test_validators.py`, `tests/unit/test_csv_parsing.py`
- Documentation: `DEMO_RUNBOOK.md`, `GCP_SETUP_GUIDE.md`, `README.md`, `REPO_SUMMARY.md`

How to render locally

1. View the Mermaid block on GitHub (renders automatically) or use a Mermaid live editor: https://mermaid.live
2. To convert to PNG/SVG locally, install `mmdc` (Mermaid CLI) via npm and run:

```bash
npm install -g @mermaid-js/mermaid-cli
mmdc -i ARCHITECTURE_DIAGRAM.md -o architecture.png
```


# GCP Setup Guide — Macys Airflow Demo Terraform Project

## Prerequisites

- macOS (Apple Silicon or Intel)
- A Google Cloud organization with a billing account
- An admin who can grant project-level permissions
- A Jira Cloud account with API token (for alerting integration)

---

## Step 1: Install Required Tools

### Install Google Cloud SDK

```bash
brew install --cask google-cloud-sdk
```

After install, restart your terminal or run:

```bash
source "$(brew --prefix)/share/google-cloud-sdk/path.zsh.inc"
```

Verify:

```bash
gcloud --version
```

### Install Terraform

```bash
brew tap hashicorp/tap
brew install hashicorp/tap/terraform
```

Verify (must be >= 1.5.0):

```bash
terraform --version
```

---

## Step 2: Authenticate with GCP

### Login to gcloud

```bash
gcloud auth login
```

### Set Application Default Credentials (for Terraform)

```bash
gcloud auth application-default login --no-launch-browser
```

This will print a URL — copy it, open in your browser, sign in, grant all permissions, then paste the authorization code back into the terminal.

> **Important:** You must use `--no-launch-browser` (not `--no-browser`). The flag name changed in newer gcloud versions. If you get a `redirect_uri` error in the browser, update gcloud first: `gcloud components update`

> **Alternative (Service Account Key):** If browser auth keeps failing, use a service account key instead:
> ```bash
> gcloud iam service-accounts create terraform-deployer \
>   --display-name="Terraform Deployer" --project=aaic-opsrabbit-demo
> gcloud iam service-accounts keys create ~/terraform-key.json \
>   --iam-account=terraform-deployer@aaic-opsrabbit-demo.iam.gserviceaccount.com
> gcloud projects add-iam-policy-binding aaic-opsrabbit-demo \
>   --member="serviceAccount:terraform-deployer@aaic-opsrabbit-demo.iam.gserviceaccount.com" \
>   --role="roles/owner"
> export GOOGLE_APPLICATION_CREDENTIALS=~/terraform-key.json
> ```

---

## Step 3: GCP Project Setup

### Option A: Admin creates the project for you (recommended)

Ask your org admin to:

1. Create the project `aaic-opsrabbit-demo`
2. Link a billing account to it
3. Grant you `roles/owner` on the project:

```bash
gcloud projects add-iam-policy-binding aaic-opsrabbit-demo \
  --member="user:YOUR_EMAIL@yourdomain.com" \
  --role="roles/owner"
```

### Option B: Create the project yourself

You need `roles/resourcemanager.projectCreator` at the org level. Ask your org admin to grant it:

```bash
# Org admin runs this:
gcloud organizations add-iam-policy-binding YOUR_ORG_ID \
  --member="user:YOUR_EMAIL@yourdomain.com" \
  --role="roles/resourcemanager.projectCreator"
```

Then create the project:

```bash
gcloud projects create aaic-opsrabbit-demo --name="AAIC OpsRabbit Demo"
gcloud billing projects link aaic-opsrabbit-demo --billing-account=YOUR_BILLING_ACCOUNT_ID
```

### Set the active project

```bash
gcloud config set project aaic-opsrabbit-demo
```

---

## Step 4: Verify Your Permissions

If you have `roles/owner`, you're all set. Otherwise, you need these specific roles on the project:

| Role | Why |
|------|-----|
| `roles/composer.admin` | Create/manage Composer environments |
| `roles/iam.serviceAccountAdmin` | Create service accounts |
| `roles/iam.serviceAccountUser` | Assign service accounts to resources |
| `roles/resourcemanager.projectIamAdmin` | Bind IAM roles |
| `roles/bigquery.admin` | Create datasets and tables |
| `roles/storage.admin` | Create GCS buckets |
| `roles/serviceusage.serviceUsageAdmin` | Enable GCP APIs |

Check your current roles:

```bash
gcloud projects get-iam-policy aaic-opsrabbit-demo \
  --flatten="bindings[].members" \
  --filter="bindings.members:YOUR_EMAIL@yourdomain.com" \
  --format="table(bindings.role)"
```

> **Note:** If this command fails with a permission error, you likely don't have IAM policy read access. Ask your admin to confirm your roles.

---

## Step 5: Understand the Service Account Chain

Terraform creates and uses multiple service accounts:

| Identity | Who creates it | Purpose |
|----------|---------------|---------|
| Your GCP user (e.g. `you@company.com`) | — | Runs `terraform apply`, creates all resources |
| `update-inventory-composer-sa` | Terraform (`iam.tf`) | Runtime SA for Composer — runs Airflow tasks, accesses BQ & GCS |
| `jira-ticket-creator` | Terraform (`cloud_function.tf`) | SA for the Cloud Function that creates Jira tickets |
| `service-PROJECT_NUMBER@cloudcomposer-accounts.iam.gserviceaccount.com` | Google (auto-created) | Google-managed agent that manages Composer infrastructure |
| `service-PROJECT_NUMBER@gcp-sa-monitoring-notification.iam.gserviceaccount.com` | Google (auto-created) | Monitoring notification SA that publishes to Pub/Sub |

---

## Step 6: Configure Terraform Variables

Edit `terraform.tfvars` with your values:

```hcl
project_id        = "aaic-opsrabbit-demo"
region            = "us-central1"
composer_env_name = "update-inventory"
bucket_name       = "aaic-opsrabbit-demo-retail-inventory-001"   # must be globally unique
bq_dataset        = "retail_analytics"

# Jira integration
jira_base_url    = "https://YOUR_ORG.atlassian.net"
jira_project_key = "OR"
jira_user_email  = "YOUR_JIRA_EMAIL"
jira_api_token   = "YOUR_JIRA_API_TOKEN"
```

> **Security:** The `jira_api_token` is marked as sensitive in Terraform and stored in GCP Secret Manager. Never commit it to git.

---

## Step 7: Deploy with Terraform

### Initialize (first time or after adding providers)

```bash
terraform init -upgrade
```

> Use `-upgrade` whenever new providers are added (e.g. `hashicorp/time`, `hashicorp/archive`, `google-beta`).

### Preview changes

```bash
terraform plan
```

### Apply

```bash
terraform apply
```

Type `yes` when prompted.

> **Timing:** The Composer environment takes ~20-25 minutes to create. Terraform includes a 90-second IAM propagation wait before starting the Composer environment to avoid race conditions.

### What Terraform Creates

| Resource | File | Purpose |
|----------|------|---------|
| GCP APIs (15 services) | `main.tf` | Enables all required APIs |
| Composer environment | `composer.tf` | Airflow orchestration platform |
| GCS bucket | `storage.tf` | Inventory CSV storage |
| BigQuery dataset + 2 tables | `bigquery.tf` | staging_inventory, inventory_summary |
| Service accounts + IAM bindings | `iam.tf` | Composer SA, agent permissions |
| Secret Manager secret | `jira_alerting.tf` | Stores Jira API token securely |
| Pub/Sub topic | `jira_alerting.tf` | Alert notification channel |
| Cloud Monitoring alert policy | `jira_alerting.tf` | Watches for Composer task failures |
| Cloud Function (2nd gen) | `cloud_function.tf` | Creates Jira tickets on alert |
| Function source bucket | `cloud_function.tf` | Stores Cloud Function zip |

### Verify outputs

After apply completes, you'll see:

- `inventory_bucket` — GCS bucket name
- `bigquery_dataset` — BigQuery dataset ID
- `composer_environment_name` — Composer environment name
- `composer_dag_gcs_prefix` — GCS path to upload your DAG files
- `composer_service_account` — SA email used by Composer

---

## Step 8: Upload DAG and Test Data

### Upload the DAG to Composer

```bash
gsutil cp inventory_pipeline_dag.py $(terraform output -raw composer_dag_gcs_prefix)/
```

Wait 1-2 minutes for Composer to pick up the DAG. Verify in the Airflow UI or check logs.

### Upload test CSV data

The DAG expects a file named `inventory_YYYY_MM_DD.csv` matching the execution date. Upload with today's date:

```bash
gsutil cp inventory_2026_03_10.csv \
  gs://aaic-opsrabbit-demo-retail-inventory-001/inventory/inventory_$(date +%Y_%m_%d).csv
```

### Trigger the pipeline

```bash
gcloud composer environments run update-inventory \
  --location us-central1 \
  dags trigger -- daily_inventory_pipeline
```

### Access Airflow UI

```bash
gcloud composer environments describe update-inventory \
  --location=us-central1 \
  --format="value(config.airflowUri)"
```

Open the returned URL in your browser to monitor the DAG run.

---

## Step 9: Test the Failure Scenario (OpsRabbit RCA Demo)

This triggers the full alerting chain: DAG fails → Cloud Logging → Alert → Pub/Sub → Cloud Function → Jira ticket.

### 1. Upload bad CSV data

```bash
gsutil cp bad_inventory.csv \
  gs://aaic-opsrabbit-demo-retail-inventory-001/inventory/inventory_$(date +%Y_%m_%d).csv
```

The `bad_inventory.csv` contains a negative inventory count (`-5`) which the `validate_data` task will reject.

### 2. Trigger the DAG

```bash
gcloud composer environments run update-inventory \
  --location us-central1 \
  dags trigger -- daily_inventory_pipeline
```

### 3. Verify the failure chain

Monitor each step:

| Step | How to check |
|------|-------------|
| DAG failure | Airflow UI — `validate_data` task should be red |
| Error in Cloud Logging | GCP Console → Logging → filter by `daily_inventory_pipeline` |
| Alert fired | GCP Console → Monitoring → Alerting → check for open incident |
| Cloud Function triggered | `gcloud functions logs read jira-ticket-creator --region=us-central1 --gen2 --limit=10` |
| Jira ticket created | Check your Jira project `OR` for a new Bug ticket |

### Expected failure message

```
Data validation failed:
Row 1: SKU SKU1001 at STORE101 has negative inventory count (-5)
```

---

## Step 10: OpsRabbit Integration (Jira → RCA Chat)

This connects Jira to OpsRabbit so that when a pipeline failure creates a Jira ticket, OpsRabbit automatically opens an RCA investigation chat.

### 1. Configure OpsRabbit Jira Integration

In OpsRabbit UI → Integrations → Jira → Configure:

| Field | Value |
|-------|-------|
| Jira Server URL | `https://opsrabbit.atlassian.net` |
| Authentication Method | `token` |
| Username | Your Jira email (same one used for API token) |
| API Token | Your Jira API token |
| Auto Comment | Toggle ON |
| Auto Comment Mode | `first_reply` |
| Verify SSL | ON (if using HTTPS) |

Save the config.

### 2. Set Up HTTPS for OpsRabbit (if no domain/SSL)

Jira Cloud requires HTTPS for webhooks. If your OpsRabbit VM doesn't have SSL, use ngrok:

```bash
# On the OpsRabbit VM:
sudo snap install ngrok
ngrok config add-authtoken YOUR_NGROK_AUTH_TOKEN
ngrok http 3000
```

Get your auth token from: https://dashboard.ngrok.com/get-started/your-authtoken

ngrok will show a forwarding URL like:
```
https://abc123.ngrok-free.dev -> http://localhost:3000
```

> **Note:** The ngrok URL changes every restart (free plan). For production, use a domain with Let's Encrypt SSL.

### 3. Create Jira Webhook

Go to: `https://opsrabbit.atlassian.net/plugins/servlet/webhooks`

Create a new webhook:

| Field | Value |
|-------|-------|
| URL | `https://YOUR_NGROK_URL/webhooks/jira` |
| JQL Filter | `All Issues` (or `project = OR`) |
| Issue: created | ✓ |
| Issue: updated | ✓ |
| Comment: created | ✓ |

Leave everything else unchecked. Save.

### 4. Enable the Integration

In OpsRabbit → Integrations → toggle the Jira integration ON.

### 5. Verify the Full Chain

The complete flow:

```
Bad CSV → Composer DAG fails → Cloud Logging → Alert → Cloud Function → Jira ticket → Jira webhook → OpsRabbit RCA chat
```

Check OpsRabbit's Chat section after triggering a pipeline failure — a new RCA thread should appear.

---

## Step 11: Pipeline Architecture

```
                          Airflow DAG (Composer)
┌─────────────┐     ┌──────────────┐     ┌───────────────┐     ┌────────────────┐
│ load_csv_    │────▶│ validate_    │────▶│ load_to_      │────▶│ build_         │
│ from_gcs     │     │ data         │     │ staging       │     │ summary        │
└─────────────┘     └──────┬───────┘     └───────────────┘     └────────────────┘
                           │ (fails on bad data)
                           ▼
                    ┌──────────────┐
                    │ Cloud        │
                    │ Logging      │
                    └──────┬───────┘
                           │
                           ▼
                    ┌──────────────┐
                    │ Cloud        │
                    │ Monitoring   │
                    │ Alert Policy │
                    └──────┬───────┘
                           │
                           ▼
                    ┌──────────────┐
                    │ Pub/Sub      │
                    │ Topic        │
                    └──────┬───────┘
                           │
                           ▼
                    ┌──────────────┐
                    │ Cloud        │
                    │ Function     │
                    └──────┬───────┘
                           │
                           ▼
                    ┌──────────────┐
                    │ Jira         │
                    │ (Bug Ticket) │
                    └──────┬───────┘
                           │ (webhook)
                           ▼
                    ┌──────────────┐
                    │ OpsRabbit    │
                    │ (RCA Chat)   │
                    └──────────────┘
```

---

## Step 12: OpsRabbit Agent — Non-Interactive GCP Auth (Service Account Key)

This gives OpsRabbit's agent the ability to interact with GCP (Composer, BigQuery, GCS, Logging) without requiring a human to log in.

### 1. Service Account (already created via Terraform)

The `opsrabbit-agent` service account was created with these roles:

| Role | Purpose |
|------|---------|
| `roles/composer.admin` | Manage Composer environments, trigger DAGs |
| `roles/storage.admin` | Read/write GCS buckets |
| `roles/bigquery.admin` | Query and manage BigQuery |
| `roles/logging.viewer` | Read Cloud Logging logs |
| `roles/monitoring.viewer` | Read monitoring metrics and alerts |

### 2. Copy the key to the OpsRabbit VM

From your Mac (in the `~/keys` directory where the key and pem files are):

```bash
scp -i opsrabbit_deploy.pem opsrabbit-agent-key.json ubuntu@35.154.110.110:~/opsrabbit-agent-key.json
```

### 3. Install gcloud CLI on the VM

```bash
sudo snap install google-cloud-cli --classic
```

> `--classic` is required because gcloud needs full system access (file system, network, home directory).

### 4. Set key file permissions on the VM

```bash
chmod 644 ~/opsrabbit-agent-key.json
```

### 5. Update docker-compose.yml

Add/uncomment these lines in the daemon service:

```yaml
services:
  daemon:
    user: "1000:1000"    # Must match the UID the agent subprocess uses
    volumes:
      - ~/opsrabbit-agent-key.json:/app/opsrabbit-agent-key.json:ro
      - ${HOME}/.config/gcloud:/home/opsbot/.config/gcloud:rw
    environment:
      - GOOGLE_APPLICATION_CREDENTIALS=/app/opsrabbit-agent-key.json
      - GOOGLE_CLOUD_PROJECT=aaic-opsrabbit-demo
      - CLOUDSDK_CONFIG=/home/opsbot/.config/gcloud
```

### 6. Prepare gcloud config directory on the VM host

```bash
sudo rm -rf ~/.config/gcloud/
mkdir -p ~/.config/gcloud
sudo chown -R 1000:1000 ~/.config/gcloud/
chmod -R 755 ~/.config/gcloud/
```

### 7. Start the containers

```bash
cd ~/OpsBot
docker compose down && docker compose up -d
```

### 8. Authenticate gcloud inside the container (as uid 1000)

```bash
docker exec -it -u 1000 opsbot-daemon \
  gcloud auth activate-service-account \
  --key-file=/app/opsrabbit-agent-key.json

docker exec -it -u 1000 opsbot-daemon \
  gcloud config set project aaic-opsrabbit-demo
```

### 9. Fix ownership after auth (gcloud creates files as the calling user)

```bash
docker exec -it -u 0 opsbot-daemon bash -c "
  chmod -R 777 /home/opsbot/.config/gcloud/ &&
  chown -R 1000:1000 /home/opsbot/.config/gcloud/
"
```

### 10. Verify

```bash
docker exec -it -u 1000 opsbot-daemon gcloud auth list
docker exec -it -u 1000 opsbot-daemon gcloud projects list
docker exec -it -u 1000 opsbot-daemon gcloud logging read 'logName=~"airflow"' --project=aaic-opsrabbit-demo --limit=3 --freshness=30d --format=json
```

Then test from OpsRabbit UI — ask it to run a gcloud command through its agent.

> **Key insight:** The root cause of most gcloud permission errors in Docker is UID mismatch. The container's agent subprocess runs as uid 1000, but the terminal user may be uid 1001. Always use `user: "1000:1000"` in docker-compose and authenticate as uid 1000.

### Quick Recovery (if gcloud breaks after a container restart)

```bash
# Fix permissions
docker exec -it -u 0 opsbot-daemon bash -c "
  chown -R 1000:1000 /home/opsbot/.config/gcloud/ &&
  chmod -R 777 /home/opsbot/.config/gcloud/
"

# Re-authenticate
docker exec -it -u 1000 opsbot-daemon \
  gcloud auth activate-service-account \
  --key-file=/app/opsrabbit-agent-key.json
```

### Log Queries (Composer logs persist after deletion)

Per [Google's official docs](https://docs.cloud.google.com/composer/docs/composer-3/delete-environments), deleting a Composer environment does NOT delete Cloud Logging logs. Logs are retained for 30 days. Use `--freshness=30d` flag since `gcloud logging read` defaults to last 24 hours only:

```bash
# All airflow logs
gcloud logging read 'logName=~"airflow"' --project=aaic-opsrabbit-demo --limit=10 --freshness=30d --format=json

# Task failure logs
gcloud logging read 'logName=~"airflow" AND (textPayload=~"failed" OR severity>=ERROR)' --project=aaic-opsrabbit-demo --limit=10 --freshness=30d --format=json
```

---

## Teardown (when done)

```bash
terraform destroy
```

Type `yes` to confirm. This removes all resources created by Terraform.

---

## Quick Reference — Common Issues

| Issue | Fix |
|-------|-----|
| `command not found: gcloud` | Restart terminal or `source "$(brew --prefix)/share/google-cloud-sdk/path.zsh.inc"` |
| `command not found: terraform` | `brew install hashicorp/tap/terraform` |
| Browser auth fails with `redirect_uri` error | Use `--no-launch-browser` flag and/or run `gcloud components update` |
| `cloud-platform scope not consented` | Re-run auth and check all permission boxes in the consent screen |
| No permission to create project | Ask org admin for `roles/resourcemanager.projectCreator` at org level |
| No permission to access project | Ask admin to grant `roles/owner` on the project |
| Terraform can't find credentials | Run `gcloud auth application-default login --no-launch-browser` |
| Invalid Composer image version | Check valid versions in the error message and update `composer.tf` |
| Composer agent SA doesn't exist | Ensure `depends_on = [google_project_service.services]` is set on the IAM binding |
| Composer fails with missing permissions | IAM propagation delay — the `time_sleep` resource (90s) in `composer.tf` handles this |
| Monitoring notification SA doesn't exist | Uses `google_project_service_identity` to auto-create it before granting Pub/Sub access |
| Eventarc API not enabled | Added to `main.tf` API list — run `terraform init -upgrade` then `apply` |
| Cloud Function creation fails | Ensure all APIs are enabled and `depends_on` is set for `google_project_service.services` |
| DAG fails with file not found (404) | CSV filename must match execution date: `inventory_YYYY_MM_DD.csv` |
| Can't check your own IAM roles | Ask your admin to confirm — you may lack `resourcemanager.projects.getIamPolicy` |
| Jira 401 / "Client must be authenticated" | See Jira Credential Troubleshooting section below |
| gcloud in container: "Permission denied" on credentials.db | UID mismatch — authenticate as uid 1000: `docker exec -it -u 1000 opsbot-daemon gcloud auth list`. See Step 12 |
| gcloud in container: "Read-only file system" | The `/app` volume is mounted as `:ro`. Copy files to `/tmp` or use host-mounted gcloud config |
| gcloud logging read returns empty `[]` | Add `--freshness=30d` flag — default is 24 hours only |

## Jira Credential Troubleshooting

If the Cloud Function logs show `401 - "You do not have permission to create issues"` or the curl test returns `"Client must be authenticated"`:

### 1. Verify the email matches the Atlassian account

The email in `terraform.tfvars` must be the exact email shown at:
https://id.atlassian.com/manage-profile/email

Sometimes people use a different email for Atlassian than their work email.

### 2. Generate a fresh API token

The token is tied to the account that created it — it must be used with that same account's email.

1. Go to https://id.atlassian.com/manage-profile/security/api-tokens
2. Click "Create API token"
3. Label it (e.g. "Composer Alerts")
4. Copy the token immediately

### 3. Check if API tokens are allowed

Your org admin may have disabled API token access:
https://admin.atlassian.com → Security → Authentication policies → ensure "API token" is not blocked.

### 4. Test credentials with curl

```bash
curl -s -u "YOUR_EMAIL:YOUR_TOKEN" "https://YOUR_ORG.atlassian.net/rest/api/3/myself"
```

- If it returns user info JSON → credentials work, check Jira project permissions
- If it returns `"Client must be authenticated"` → email/token combo is invalid

### 5. Test issue creation

```bash
curl -s -X POST -u "YOUR_EMAIL:YOUR_TOKEN" -H "Content-Type: application/json" -d '{"fields":{"project":{"key":"OR"},"summary":"Test ticket","issuetype":{"name":"Bug"}}}' "https://YOUR_ORG.atlassian.net/rest/api/3/issue"
```

### 6. After fixing credentials

Update `terraform.tfvars` with the correct email and token, then:

```bash
terraform apply
```

Re-trigger the DAG to test the full chain again.

## File Structure

```
.
├── main.tf                      # Provider config, API enablement
├── versions.tf                  # Terraform and provider versions
├── variables.tf                 # Input variables
├── terraform.tfvars             # Variable values (don't commit secrets)
├── iam.tf                       # Service accounts and IAM bindings
├── composer.tf                  # Cloud Composer environment
├── storage.tf                   # GCS bucket for inventory data
├── bigquery.tf                  # BigQuery dataset and tables
├── jira_alerting.tf             # Secret Manager, Pub/Sub, alert policy
├── cloud_function.tf            # Cloud Function (2nd gen) for Jira
├── cloud_function/
│   ├── main.py                  # Function code — creates Jira tickets
│   └── requirements.txt         # Python dependencies
├── outputs.tf                   # Terraform outputs
├── inventory_pipeline_dag.py    # Airflow DAG file
├── inventory_2026_03_10.csv     # Good test data (10 rows)
├── bad_inventory.csv            # Bad test data (negative count)
└── GCP_SETUP_GUIDE.md           # This file
```

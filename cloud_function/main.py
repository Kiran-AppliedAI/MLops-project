"""Cloud Function: Creates a Jira ticket when a Composer alert fires."""

import base64
import json
import os
import requests
from google.cloud import secretmanager


def get_jira_token():
    """Retrieve Jira API token from Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    project_id = os.environ["GCP_PROJECT"]
    name = f"projects/{project_id}/secrets/jira-api-token/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


def create_jira_ticket(event, context):
    """Triggered by Pub/Sub message from Cloud Monitoring alert."""
    # Decode the Pub/Sub message
    if "data" in event:
        pubsub_data = base64.b64decode(event["data"]).decode("utf-8")
        alert_payload = json.loads(pubsub_data)
    else:
        alert_payload = {}

    # Extract alert details
    policy_name = alert_payload.get("incident", {}).get("policy_name", "Unknown Policy")
    summary = alert_payload.get("incident", {}).get("summary", "No summary available")
    state = alert_payload.get("incident", {}).get("state", "unknown")
    incident_url = alert_payload.get("incident", {}).get("url", "")
    
    # Log the full payload for debugging
    print(f"Alert payload keys: {list(alert_payload.get('incident', {}).keys())}")
    print(f"Summary: {summary}")

    # Extract DAG name from the alert summary or by querying recent logs
    dag_name = "unknown_pipeline"
    
    # First try: check if summary contains a known DAG name
    full_payload_str = json.dumps(alert_payload)
    for known_dag in ["inventory_store_reconciliation", "inventory_cross_store_analysis",
                      "daily_inventory_pipeline", "inventory_batch_processor",
                      "pricing_margin_calculator", "store_replenishment_optimizer",
                      "inventory_allocation_engine", "fulfillment_routing_pipeline"]:
        if known_dag in full_payload_str:
            dag_name = known_dag
            break
    
    # Second try: query Cloud Logging for most recent failure
    if dag_name == "unknown_pipeline":
        try:
            from google.cloud import logging as cloud_logging
            import re as re_module
            log_client = cloud_logging.Client(project=os.environ["GCP_PROJECT"])
            
            filter_str = (
                'resource.type="cloud_composer_environment" '
                'textPayload=~"Marking task as FAILED. dag_id="'
            )
            
            for entry in log_client.list_entries(filter_=filter_str, max_results=1, order_by=cloud_logging.DESCENDING):
                text = entry.payload if isinstance(entry.payload, str) else str(entry.payload)
                dag_match = re_module.search(r'dag_id=([a-zA-Z0-9_]+)', text)
                if dag_match:
                    dag_name = dag_match.group(1)
                break
        except Exception as e:
            print(f"Log query failed: {e}")
    
    print(f"Detected DAG: {dag_name}")

    # Only create tickets for opened incidents (not resolved)
    if state != "open":
        print(f"Incident state is '{state}', skipping Jira ticket creation.")
        return "Skipped - incident not open", 200

    # Build Jira ticket — title shows only DAG name
    jira_url = os.environ["JIRA_BASE_URL"]
    jira_email = os.environ["JIRA_USER_EMAIL"]
    jira_project = os.environ["JIRA_PROJECT_KEY"]
    jira_token = get_jira_token()

    description = (
        f"*Environment:* {os.environ['GCP_PROJECT']}\n"
        f"*DAG:* {dag_name}\n"
        f"*Summary:* {summary}\n"
        f"*GCP Incident:* {incident_url}\n\n"
        f"This ticket was auto-created by Cloud Monitoring."
    )

    ticket_data = {
        "fields": {
            "project": {"key": jira_project},
            "summary": f"[Composer Alert] Composer DAG Task Failure for {dag_name}",
            "description": {
                "type": "doc",
                "version": 1,
                "content": [
                    {
                        "type": "paragraph",
                        "content": [
                            {"type": "text", "text": description}
                        ],
                    }
                ],
            },
            "issuetype": {"name": "Bug"},
        }
    }

    response = requests.post(
        f"{jira_url}/rest/api/3/issue",
        json=ticket_data,
        auth=(jira_email, jira_token),
        headers={"Content-Type": "application/json"},
    )

    if response.status_code == 201:
        ticket_key = response.json().get("key")
        print(f"Jira ticket created: {ticket_key}")

        # Notify OpsRabbit directly (bypass Jira webhook reliability issues)
        opsrabbit_url = os.environ.get("OPSRABBIT_WEBHOOK_URL", "")
        if opsrabbit_url:
            try:
                webhook_payload = {
                    "webhookEvent": "jira:issue_created",
                    "issue": {
                        "key": ticket_key,
                        "fields": {
                            "summary": f"[Composer Alert] Composer DAG Task Failure for {dag_name}",
                            "issuetype": {"name": "Bug"},
                            "project": {"key": jira_project},
                            "description": description,
                        },
                    },
                }
                opsrabbit_resp = requests.post(
                    opsrabbit_url,
                    json=webhook_payload,
                    headers={"Content-Type": "application/json"},
                    timeout=10,
                    verify=False,
                )
                print(f"OpsRabbit notified: {opsrabbit_resp.status_code}")
            except Exception as e:
                print(f"OpsRabbit notification failed (non-fatal): {e}")

        return f"Created {ticket_key}", 200
    else:
        print(f"Failed to create Jira ticket: {response.status_code} - {response.text}")
        return f"Failed: {response.status_code}", 500

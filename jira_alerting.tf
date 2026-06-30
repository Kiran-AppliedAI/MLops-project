# --- Secret Manager for Jira API Token ---

resource "google_secret_manager_secret" "jira_api_token" {
  secret_id = "jira-api-token"

  replication {
    auto {}
  }

  depends_on = [google_project_service.services]
}

resource "google_secret_manager_secret_version" "jira_api_token" {
  secret      = google_secret_manager_secret.jira_api_token.id
  secret_data = var.jira_api_token
}

# --- Pub/Sub Topic for Alert Notifications ---

resource "google_pubsub_topic" "composer_alerts" {
  name = "composer-pipeline-alerts"

  depends_on = [google_project_service.services]
}

# --- Cloud Monitoring Notification Channel (Pub/Sub) ---

resource "google_monitoring_notification_channel" "pubsub" {
  display_name = "Composer Alerts Pub/Sub"
  type         = "pubsub"

  labels = {
    topic = google_pubsub_topic.composer_alerts.id
  }

  depends_on = [google_project_service.services]
}

# Grant Monitoring permission to publish to the topic
# The monitoring notification SA is auto-created when the Monitoring API is used
resource "google_project_service_identity" "monitoring_sa" {
  provider = google-beta
  project  = var.project_id
  service  = "monitoring.googleapis.com"

  depends_on = [google_project_service.services]
}

resource "google_pubsub_topic_iam_member" "monitoring_publisher" {
  topic  = google_pubsub_topic.composer_alerts.name
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${google_project_service_identity.monitoring_sa.email}"
}

# --- Log-Based Alert Policy ---

resource "google_monitoring_alert_policy" "composer_task_failure" {
  display_name = "Composer DAG Task Failure"
  combiner     = "OR"

  conditions {
    display_name = "Composer task failed"

    condition_matched_log {
      filter = <<-EOT
        resource.type="cloud_composer_environment"
        resource.labels.environment_name="${var.composer_env_name}"
        (severity>=ERROR OR (severity>=WARNING AND (textPayload=~"Broken DAG" OR textPayload=~"No module named")))
        (textPayload=~"Task failed with exception" OR
         textPayload=~"Marking task as FAILED" OR
         textPayload=~"Task exited with return code" OR
         textPayload=~"Task timed out" OR
         textPayload=~"OOMKilled" OR
         textPayload=~"Duplicate data detected" OR
         textPayload=~"Log file does not exist" OR
         textPayload=~"task_id=.*state=failed" OR
         textPayload=~"Broken DAG" OR
         textPayload=~"No module named")
      EOT
    }
  }

  alert_strategy {
    notification_rate_limit {
      period = "300s"
    }

    auto_close = "1800s"
  }

  notification_channels = [
    google_monitoring_notification_channel.pubsub.name
  ]

  documentation {
    content   = "A task has failed in Composer environment `${var.composer_env_name}`. Check Cloud Logging for error details."
    mime_type = "text/markdown"
  }
}

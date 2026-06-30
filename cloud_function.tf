# --- Cloud Function Service Account ---

resource "google_service_account" "jira_function_sa" {
  account_id   = "jira-ticket-creator"
  display_name = "Jira Ticket Creator Cloud Function SA"
}

resource "google_project_iam_member" "function_secret_accessor" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.jira_function_sa.email}"
}

resource "google_project_iam_member" "function_log_viewer" {
  project = var.project_id
  role    = "roles/logging.viewer"
  member  = "serviceAccount:${google_service_account.jira_function_sa.email}"
}

# --- Zip the function source ---

data "archive_file" "function_source" {
  type        = "zip"
  source_dir  = "${path.module}/cloud_function"
  output_path = "${path.module}/cloud_function.zip"
}

# --- Upload function source to GCS ---

resource "google_storage_bucket" "function_bucket" {
  name          = "${var.project_id}-cloud-functions"
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true

  depends_on = [google_project_service.services]
}

resource "google_storage_bucket_object" "function_zip" {
  name   = "jira-ticket-creator-${data.archive_file.function_source.output_md5}.zip"
  bucket = google_storage_bucket.function_bucket.name
  source = data.archive_file.function_source.output_path
}

# --- Cloud Function (2nd gen) ---

resource "google_cloudfunctions2_function" "jira_ticket_creator" {
  name     = "jira-ticket-creator"
  location = var.region

  depends_on = [google_project_service.services]

  build_config {
    runtime     = "python311"
    entry_point = "create_jira_ticket"

    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = google_storage_bucket_object.function_zip.name
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "256M"
    timeout_seconds    = 60

    service_account_email = google_service_account.jira_function_sa.email

    environment_variables = {
      GCP_PROJECT          = var.project_id
      JIRA_BASE_URL        = var.jira_base_url
      JIRA_USER_EMAIL      = var.jira_user_email
      JIRA_PROJECT_KEY     = var.jira_project_key
      OPSRABBIT_WEBHOOK_URL = var.opsrabbit_webhook_url
    }
  }

  event_trigger {
    trigger_region = var.region
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.composer_alerts.id
    retry_policy   = "RETRY_POLICY_DO_NOT_RETRY"
  }
}

# Wait for IAM propagation before creating Composer environment
resource "time_sleep" "wait_for_iam" {
  create_duration = "90s"

  depends_on = [
    google_project_iam_member.composer_agent_v2,
    google_service_account_iam_member.composer_agent_sa_user
  ]
}

resource "google_composer_environment" "airflow" {
  name   = var.composer_env_name
  region = var.region

  depends_on = [
    google_project_service.services,
    time_sleep.wait_for_iam
  ]

  config {
    environment_size = "ENVIRONMENT_SIZE_SMALL"

    software_config {
      image_version = "composer-2.16.6-airflow-2.10.5"
    }

    node_config {
      service_account = google_service_account.composer_sa.email
    }

    workloads_config {
      scheduler {
        cpu        = 1
        memory_gb  = 2
        storage_gb = 1
        count      = 1
      }

      web_server {
        cpu        = 1
        memory_gb  = 2
        storage_gb = 1
      }

      worker {
        cpu        = 1
        memory_gb  = 2
        storage_gb = 10
        min_count  = 1
        max_count  = 2
      }
    }
  }
}

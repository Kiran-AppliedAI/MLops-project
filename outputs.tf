output "inventory_bucket" {
  value = google_storage_bucket.inventory_bucket.name
}

output "bigquery_dataset" {
  value = google_bigquery_dataset.retail_dataset.dataset_id
}

output "composer_environment_name" {
  value = google_composer_environment.airflow.name
}

output "composer_dag_gcs_prefix" {
  value = google_composer_environment.airflow.config[0].dag_gcs_prefix
}

output "composer_service_account" {
  value = google_service_account.composer_sa.email
}

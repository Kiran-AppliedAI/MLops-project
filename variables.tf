variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "composer_env_name" {
  description = "Composer environment name"
  type        = string
  default     = "update-inventory"
}

variable "bucket_name" {
  description = "Globally unique GCS bucket name"
  type        = string
}

variable "bq_dataset" {
  description = "BigQuery dataset name"
  type        = string
  default     = "retail_analytics"
}

variable "jira_base_url" {
  description = "Jira instance URL (e.g. https://yourorg.atlassian.net)"
  type        = string
}

variable "jira_project_key" {
  description = "Jira project key for ticket creation"
  type        = string
  default     = "OR"
}

variable "jira_user_email" {
  description = "Jira user email for API authentication"
  type        = string
}

variable "jira_api_token" {
  description = "Jira API token"
  type        = string
  sensitive   = true
}

variable "opsrabbit_webhook_url" {
  description = "OpsRabbit Jira webhook URL for direct notification"
  type        = string
  default     = ""
}

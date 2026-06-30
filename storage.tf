resource "google_storage_bucket" "inventory_bucket" {
  name          = var.bucket_name
  location      = var.region

  depends_on = [google_project_service.services]
  force_destroy = true

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }
}

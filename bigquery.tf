resource "google_bigquery_dataset" "retail_dataset" {
  dataset_id = var.bq_dataset
  location   = var.region

  depends_on = [google_project_service.services]
}

resource "google_bigquery_table" "staging_inventory" {
  dataset_id = google_bigquery_dataset.retail_dataset.dataset_id
  table_id   = "staging_inventory"

  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "last_updated"
  }

  clustering = ["department", "store_id"]

  schema = jsonencode([
    { name = "sku_id", type = "STRING", mode = "REQUIRED" },
    { name = "store_id", type = "STRING", mode = "REQUIRED" },
    { name = "department", type = "STRING", mode = "REQUIRED" },
    { name = "category", type = "STRING", mode = "REQUIRED" },
    { name = "product_name", type = "STRING", mode = "REQUIRED" },
    { name = "size", type = "STRING", mode = "NULLABLE" },
    { name = "color", type = "STRING", mode = "NULLABLE" },
    { name = "unit_cost", type = "FLOAT64", mode = "REQUIRED" },
    { name = "retail_price", type = "FLOAT64", mode = "REQUIRED" },
    { name = "inventory_count", type = "INT64", mode = "REQUIRED" },
    { name = "reorder_point", type = "INT64", mode = "REQUIRED" },
    { name = "warehouse_id", type = "STRING", mode = "REQUIRED" },
    { name = "last_received_date", type = "DATE", mode = "NULLABLE" },
    { name = "last_sold_date", type = "DATE", mode = "NULLABLE" },
    { name = "seasonal_flag", type = "STRING", mode = "NULLABLE" },
    { name = "last_updated", type = "DATE", mode = "REQUIRED" },
    { name = "ingestion_timestamp", type = "TIMESTAMP", mode = "REQUIRED" }
  ])
}

resource "google_bigquery_table" "inventory_summary" {
  dataset_id = google_bigquery_dataset.retail_dataset.dataset_id
  table_id   = "inventory_summary"

  deletion_protection = false

  schema = jsonencode([
    { name = "store_id", type = "STRING", mode = "REQUIRED" },
    { name = "total_skus", type = "INT64", mode = "REQUIRED" },
    { name = "total_units", type = "INT64", mode = "REQUIRED" },
    { name = "total_retail_value", type = "FLOAT64", mode = "REQUIRED" },
    { name = "total_cost_value", type = "FLOAT64", mode = "REQUIRED" },
    { name = "departments_represented", type = "INT64", mode = "REQUIRED" },
    { name = "skus_below_reorder", type = "INT64", mode = "REQUIRED" },
    { name = "last_updated", type = "DATE", mode = "REQUIRED" }
  ])
}

resource "google_bigquery_table" "quarantine_records" {
  dataset_id = google_bigquery_dataset.retail_dataset.dataset_id
  table_id   = "quarantine_records"

  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "pipeline_execution_date"
  }

  schema = jsonencode([
    { name = "sku_id", type = "STRING", mode = "NULLABLE" },
    { name = "store_id", type = "STRING", mode = "NULLABLE" },
    { name = "department", type = "STRING", mode = "NULLABLE" },
    { name = "category", type = "STRING", mode = "NULLABLE" },
    { name = "product_name", type = "STRING", mode = "NULLABLE" },
    { name = "inventory_count", type = "INTEGER", mode = "NULLABLE" },
    { name = "retail_price", type = "FLOAT", mode = "NULLABLE" },
    { name = "rejection_reason", type = "STRING", mode = "REQUIRED" },
    { name = "pipeline_execution_date", type = "DATE", mode = "REQUIRED" },
    { name = "raw_record", type = "STRING", mode = "REQUIRED" }
  ])
}

resource "google_bigquery_table" "inventory_by_department" {
  dataset_id = google_bigquery_dataset.retail_dataset.dataset_id
  table_id   = "inventory_by_department"

  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "report_date"
  }

  clustering = ["department"]

  schema = jsonencode([
    { name = "store_id", type = "STRING", mode = "REQUIRED" },
    { name = "department", type = "STRING", mode = "REQUIRED" },
    { name = "total_skus", type = "INTEGER", mode = "REQUIRED" },
    { name = "total_units", type = "INTEGER", mode = "REQUIRED" },
    { name = "total_retail_value", type = "FLOAT", mode = "REQUIRED" },
    { name = "avg_inventory_per_sku", type = "FLOAT", mode = "REQUIRED" },
    { name = "below_reorder_count", type = "INTEGER", mode = "REQUIRED" },
    { name = "report_date", type = "DATE", mode = "REQUIRED" }
  ])
}

resource "google_bigquery_table" "inventory_by_price_band" {
  dataset_id = google_bigquery_dataset.retail_dataset.dataset_id
  table_id   = "inventory_by_price_band"

  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "report_date"
  }

  schema = jsonencode([
    { name = "store_id", type = "STRING", mode = "REQUIRED" },
    { name = "price_band", type = "STRING", mode = "REQUIRED" },
    { name = "sku_count", type = "INTEGER", mode = "REQUIRED" },
    { name = "total_units", type = "INTEGER", mode = "REQUIRED" },
    { name = "total_retail_value", type = "FLOAT", mode = "REQUIRED" },
    { name = "report_date", type = "DATE", mode = "REQUIRED" }
  ])
}

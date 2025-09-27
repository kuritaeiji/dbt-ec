terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source = "hashicorp/google"
      version = ">= 7.0"
    }
  }
}

locals {
  dateset_suffixes = ["stg", "ref", "mart", "seed", "source"]
}

resource "google_bigquery_dataset" "dataset" {
  for_each = toset(local.dateset_suffixes)

  dataset_id = "${var.env}_${each.key}"
  location = var.location
}
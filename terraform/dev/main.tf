provider "google" {
  project = "sampleproject-440314"
  credentials = "../../serviceaccount.json"
}

module "bigquery_dataset" {
  source = "../modules/bigquery"

  env = "dev"
  location = "asia-northeast1"
}
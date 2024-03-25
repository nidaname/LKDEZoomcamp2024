variable "credentials" {
  description = "credentials for GCP Project"
  default     = "/workspaces/LKDEZoomcamp2024/1_terraform/keys/terraform_sa.json"
}

variable "project" {
  description = "GCP Project"
  default     = "onyx-descent-417702"
}

variable "project_region" {
  description = "region for GCP Project"
  default     = "asia-southeast1"
}

variable "location" {
  description = "location of any GCS resources"
  default     = "ASIA"
}

variable "bq_dataset_name" {
  description = "BQ Dataset name"
  default     = "demo_dataset_homework"
}

variable "gcs_bucket_name" {
  description = "GCS Bucket name"
  default     = "onyx-descent-417702-terra-bucket-homework"
}


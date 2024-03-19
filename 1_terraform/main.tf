terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.21.0"
    }
  }
}

provider "google" {
  project = "onyx-descent-417702"
  region  = "asia-southeast1"
}

resource "google_storage_bucket" "demo-bucket" {
  name          = "onyx-descent-417702-terra-bucket"
  location      = "ASIA"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}
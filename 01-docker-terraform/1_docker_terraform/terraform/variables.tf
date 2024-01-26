variable "credentials" {
  description = "My Credentials"
  default     = "~/.gc/terraform_key.json"
}


variable "project" {
  description = "Project"
  default     = "refined-outlet-411413"
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}


variable "bq_dataset_name" {
  description = "My BiqQuery Dataset Name"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "refined-outlet-411413-terra-bucket"
}


variable "gcs_storage_class" {
  description = "Bucker Storage Class"
  default     = "STANDARD"
}
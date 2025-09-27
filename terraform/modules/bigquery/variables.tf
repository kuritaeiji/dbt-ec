variable "env" {
  description = "Environment name (dev or prd)"
  type = string
  validation {
    condition = contains(["dev", "prd"], var.env)
    error_message = "env must be either dev or prd"
  }
}

variable "location" {
  description = "BigQuery dataset location"
  type        = string
  default     = "asia-northeast1"
}

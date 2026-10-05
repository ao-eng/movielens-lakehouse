variable "region" {
  description = "AWS region for every resource in the project"
  type        = string
  default     = "us-east-1"
}

variable "bucket_name" {
  description = "Lakehouse bucket (raw, warehouse, Athena results and Terraform state)"
  type        = string
  default     = "ao-eng"
}

variable "athena_bytes_scanned_cutoff" {
  description = "Maximum bytes a single Athena query may scan in the movielens workgroup"
  type        = number
  default     = 1073741824 # 1 GB
}

variable "aws_region" {
  description = "Region for AWS resources (N. California)"
  type        = string
  default     = "us-west-1"
}

variable "s3_bucket_name" {
  description = "My unique AWS S3 Bucket Name (Data Lake)"
  type        = string
}

variable "glue_database_name" {
  description = "AWS Glue Catalog Database Name (BigQuery Dataset Equivalent)"
  type        = string
}
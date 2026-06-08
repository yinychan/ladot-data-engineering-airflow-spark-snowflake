terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

# Configure the AWS Provider
provider "aws" {
  region = var.aws_region
}

# AWS S3 bucket to store data 
resource "aws_s3_bucket" "data_lake_bucket" {
  bucket = var.s3_bucket_name
  force_destroy = true
}

# Enable bucket versioning
resource "aws_s3_bucket_versioning" "versioning" {
  bucket = aws_s3_bucket.data_lake_bucket.id # Reference the S3 bucket created above

  versioning_configuration {
    status = "Enabled"
  }
}

# Block public access to prevent data leaks
resource "aws_s3_bucket_public_access_block" "block_public_access" {
  bucket = aws_s3_bucket.data_lake_bucket.id # Reference the S3 bucket created above

  block_public_acls = true
  block_public_policy = true
  ignore_public_acls  = true
  restrict_public_buckets = true
}

# Delete old buckets after 30 days
resource "aws_s3_bucket_lifecycle_configuration" "lifecycle_rules" {
  bucket = aws_s3_bucket.data_lake_bucket.id

  rule {
    id = "lifecycle_delete_after_30_days"
    status = "Enabled"

    expiration {
      days = 30
    }
    filter {
      prefix = ""
    }
  }
}

# Insert a dataset
resource "aws_glue_catalog_database" "dataset" {
  name = var.glue_database_name
}
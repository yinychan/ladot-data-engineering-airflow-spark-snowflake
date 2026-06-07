
## Getting Started with AWS
The starting point assumes you already have an AWS account. If not, create one.
- [Terraform with AWS official provider docs](https://registry.terraform.io/providers/hashicorp/aws/latest)

- Add terraform extension to your VS Code (search for terraform and Install the one from HashiCorp Terraform)

## Infrastructure as Code (IaC) with Terraform

- Human-readable configuration files
- Can version, reuse, and share (easy collaboration)
- Consistent workflow to provision and manage all of your infrastructure (keeps track of infrastructure)
- Ensures resources are removed. So you do not continue to be charged for them

## Connecting AWS with Terraform

### IAM users 
1. Create User button
2. Username: terraform-runner
    - Provide user access to the AWS Management Console - optional (leave unchecked)
    - Next
3. Role Mapping (GCP → AWS)
GCP Role            AWS Equivalent                                          Purpose
Storage Admin       AmazonS3FullAccess                                      Allows Terraform to create, modify, and delete S3 Buckets (Data Lakes).
BigQuery Admin      AWSGlueConsoleFullAccess and AmazonAthenaFullAccess     Allows Terraform to build the metadata catalogs, databases, and query infrastructure.
Compute Admin       AmazonEC2FullAccess                                     Allows Terraform to spin up, manage, and destroy virtual machine instances (EC2).
4. Set permissions > Permissions options > select Attach policies directly
5. Permissions policies + AmazonS3FullAccess, + AWSGlueConsoleFullAccess, + AmazonAthenaFullAccess, + AmazonEC2FullAccess
6. Next > Review > Create user

### Generate an Access Key Pair
1. Click on `terraform-runner` from your user list to open its settings.
2. Click on the "Security credentials" tab > Look for Access keys > Click "Create access key"
3. Select "Command Line Interface (CLI)" > Confirmation: I understand the above recommendation and want to proceed to create an access key. (check this box)
4. Next > Description tag value: Enter "Data engineering with terraform"
5. Store the keys in your terraform project folder (e.g. /terraform/.env or /terraform/terraform-runner_accessKeys.csv)
- Option A: Downlod .csv
- Option B: create a new .env file in your terraform folder, and Copy / Paste keys:
```
AWS_ACCESS_KEY=key_random_string_copy_pasted
AWS_SECRET_ACCESS_KEY=secret_key_random_string_copy_pasted
```
6. Make sure you .gitignore your AWS credentials first, so you don't accidentally push them to github:
```
# In .gitignore, add:

*.csv
.terraform/
terraform.tfstate*
```

### Creating Terraform Configuration Files
1. In your terminal, navigate to your terraform project directory `/terraform`, add a `variables.tf` file. Think of this file as the schema behind your terraform configuration.

```
cd terraform
touch variables.tf
```

This is where you define your inputs for AWS services:

```
# In variables.tf file:

variable "aws_region" {
  description = "Region for AWS resources (N. California)"
  type        = string
  default     = "us-west-1"
}

variable "s3_bucket_name" {
  description = "AWS S3 Bucket Name (Data Lake)"
  type        = string
}

variable "glue_database_name" {
  description = "AWS Glue Catalog Database Name"
  type        = string
}
```

2. Create a `terraform.tfvars` file to assign concrete values to your previously established input variables. This separates your deployment data from structural infastructure code so you can reuse the same `*.tf` files across different deployment environments. Think of this file as the data to your schema in `variables.tf`

```
# In your terminal
touch terraform.tfvars
```

```
# In terraform.tfvars

aws_region         = "us-west-1" # change this to the region closest to you
s3_bucket_name     = "meter-parking-DE-project-aws-data-lake-yinychan" # change this to a bucket name matching your project
glue_database_name = "la_meter_parking_data" # change this db name to match your db
```

3. Create your `main.tf` where we will be implementing the main terraform integration with AWS

```
# In your terminal
touch main.tf
```

You can reference the provider example code in [terraform docs](https://registry.terraform.io/providers/hashicorp/aws/latest/docs)for your `main.tf`.

```
# In main.tf

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
```
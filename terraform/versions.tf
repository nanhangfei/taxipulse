terraform {
  required_version = ">= 1.7"
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.94"
    }
  }
  # Production note: use a remote backend (S3+DynamoDB / Terraform Cloud)
  # so state is shared and locked. Local state is for bootstrap only.
}

provider "snowflake" {
  # Credentials from env: SNOWFLAKE_ACCOUNT / SNOWFLAKE_USER / SNOWFLAKE_PASSWORD
  # or key-pair auth via SNOWFLAKE_PRIVATE_KEY_PATH — never in .tf files.
  role = "SYSADMIN"
}

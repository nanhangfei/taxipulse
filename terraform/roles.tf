# Least-privilege functional roles; service users get exactly one role each.
resource "snowflake_account_role" "loader" {
  name    = "LOADER_ROLE"
  comment = "Airflow: write RAW_DB only"
}

resource "snowflake_account_role" "transformer" {
  name    = "TRANSFORMER_ROLE"
  comment = "dbt: read RAW_DB, write ANALYTICS_DB"
}

resource "snowflake_account_role" "reporter" {
  name    = "REPORTER_ROLE"
  comment = "Power BI: read MARTS only"
}

resource "snowflake_grant_privileges_to_account_role" "loader_raw" {
  account_role_name = snowflake_account_role.loader.name
  privileges        = ["USAGE", "CREATE TABLE"]
  on_schema {
    schema_name = "\"RAW_DB\".\"TLC\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_analytics" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE", "CREATE SCHEMA"]
  on_account_object {
    object_type = "DATABASE"
    object_name = "ANALYTICS_DB"
  }
}

resource "snowflake_grant_privileges_to_account_role" "reporter_marts" {
  account_role_name = snowflake_account_role.reporter.name
  privileges        = ["USAGE"]
  on_schema {
    schema_name = "\"ANALYTICS_DB\".\"MARTS\""
  }
}

resource "snowflake_database" "raw" {
  name    = "RAW_DB"
  comment = "Landing zone loaded by Airflow COPY INTO from MinIO gold"
}

resource "snowflake_database" "analytics" {
  name    = "ANALYTICS_DB"
  comment = "dbt-managed: staging / intermediate / marts"
}

resource "snowflake_database" "rt" {
  name    = "RT_DB"
  comment = "Flink speed-layer tables"
}

resource "snowflake_schema" "tlc" {
  database = snowflake_database.raw.name
  name     = "TLC"
}

# Staging-layer data is regenerable -> transient schema (no Fail-safe cost)
resource "snowflake_schema" "staging" {
  database   = snowflake_database.analytics.name
  name       = "STAGING"
  is_transient = true
}

resource "snowflake_schema" "marts" {
  database = snowflake_database.analytics.name
  name     = "MARTS"
}

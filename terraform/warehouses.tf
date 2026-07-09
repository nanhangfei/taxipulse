# One warehouse per workload: isolation + per-workload cost attribution.
locals {
  warehouses = {
    LOAD_WH      = { size = "XSMALL", comment = "Airflow COPY INTO" }
    TRANSFORM_WH = { size = "SMALL",  comment = "dbt builds" }
    BI_WH        = { size = "XSMALL", comment = "Power BI queries" }
  }
}

resource "snowflake_warehouse" "wh" {
  for_each            = local.warehouses
  name                = each.key
  warehouse_size      = each.value.size
  comment             = each.value.comment
  auto_suspend        = 60      # FinOps: suspend after 60s idle
  auto_resume         = true
  initially_suspended = true
}

# FinOps guardrail: hard monthly credit cap with notifications
resource "snowflake_resource_monitor" "monthly_cap" {
  name            = "PLATFORM_MONTHLY_CAP"
  credit_quota    = 100
  frequency       = "MONTHLY"
  start_timestamp = "IMMEDIATELY"
  notify_triggers = [50, 80]
  suspend_trigger = 95
}

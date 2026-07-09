FROM apache/airflow:2.9.3-python3.11

# Providers deliberately UNPINNED here: the constraints file pins the exact
# provider versions tested with Airflow 2.9.3. Adding our own pins contradicts
# it and pip's resolver melts down (ResolutionTooDeep) — same failure the old
# _PIP_ADDITIONAL_REQUIREMENTS startup install hit.
RUN pip install --no-cache-dir \
      apache-airflow-providers-snowflake \
      apache-airflow-providers-apache-spark \
      apache-airflow-providers-openlineage \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt"

# dbt (Stage 3) and Soda (Stage 5) are NOT in this image yet. Installing
# dbt-snowflake + soda-core-snowflake together is unresolvable for pip
# (ResolutionTooDeep — conflicting pins on snowflake-connector/protobuf/otel).
# When those stages arrive, give each its own venv built from a compiled
# lockfile (pip-compile), or run them in dedicated containers instead of
# inside Airflow.

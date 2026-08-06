.PHONY: up up-streaming down clean init-data lint test dbt-debug dbt-build dbt-test soda-scan \
        spark-bronze spark-silver spark-gold spark-batch snowflake-load-gold

# Load only the Snowflake/dbt vars from .env (the AIRFLOW_CONN_* URIs contain
# '&' and break shell sourcing); shared by the dbt-* targets below. The prefix
# match has no trailing '=' so it catches SNOWFLAKE_ACCOUNT etc., not just exact names.
DBT_ENV = set -a && eval "$$(grep -E "^(SNOWFLAKE_|DBT_SCHEMA)" .env)" && set +a

up:                   ## batch stack only; streaming services are behind a profile until Stage 7
	docker compose up -d --build

up-streaming:         ## Stage 7: batch stack + replay producer + Flink
	docker compose --profile streaming up -d --build

down:                 ## stop & remove all containers, batch + streaming (keeps volumes)
	docker compose --profile streaming down

clean:                ## like down, but also wipes named volumes (minio-data, airflow-pg)
	docker compose --profile streaming down -v

init-data:            ## download one month of TLC data for the replay producer & Spark
	mkdir -p data
	curl -fL -o data/yellow_tripdata_2024-01.parquet \
	  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
	curl -fL -o data/taxi_zone_lookup.csv \
	  https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv

lint:
	pip install ruff && ruff check spark_jobs producer flink_jobs airflow/dags

test:
	pytest -q tests/

dbt-debug:            ## verify the Snowflake connection (key-pair)
	bash -c '$(DBT_ENV) && cd dbt_project && ../.venv/bin/dbt debug --profiles-dir .'

dbt-build:            ## load Snowflake creds from .env (key-pair), then dbt deps + build
	bash -c '$(DBT_ENV) && cd dbt_project && ../.venv/bin/dbt deps && ../.venv/bin/dbt build --profiles-dir .'

dbt-test:             ## run dbt tests only (key-pair)
	bash -c '$(DBT_ENV) && cd dbt_project && ../.venv/bin/dbt deps && ../.venv/bin/dbt test --profiles-dir .'

soda-scan:
	soda scan -d snowflake_dev -c soda/configuration.yml soda/checks/

# ---- Stage 2: Spark batch lake path (bronze -> silver -> gold) --------------
# Submit the jobs to the standalone Spark cluster. Override YEAR/MONTH/RAW to
# process a different month, e.g. `make spark-batch YEAR=2024 MONTH=2 RAW=/opt/data/yellow_tripdata_2024-02.parquet`.
YEAR  ?= 2024
MONTH ?= 1
RAW   ?= /opt/data/yellow_tripdata_2024-01.parquet
SPARK_SUBMIT = docker compose exec -T spark-master /opt/spark/bin/spark-submit \
	--master spark://spark-master:7077 --py-files /opt/spark_jobs/common.py

spark-bronze:         ## land raw parquet -> s3a://bronze (partitioned by year/month)
	$(SPARK_SUBMIT) /opt/spark_jobs/ingest_bronze.py $(RAW) $(YEAR) $(MONTH)
spark-silver:         ## clean + quarantine -> s3a://silver, s3a://quarantine
	$(SPARK_SUBMIT) /opt/spark_jobs/transform_silver.py $(YEAR) $(MONTH)
spark-gold:           ## aggregates + trip fact -> s3a://gold
	$(SPARK_SUBMIT) /opt/spark_jobs/load_gold.py $(YEAR) $(MONTH)
spark-batch: spark-bronze spark-silver spark-gold  ## run the full bronze->silver->gold chain for YEAR/MONTH

snowflake-load-gold:  ## load s3a://gold/fct_trips -> analytics.raw.fct_trips_gold (idempotent DELETE+load per month)
	bash -c 'set -a && eval "$$(grep -E "^(SNOWFLAKE_|DBT_SCHEMA|MINIO_)" .env)" && set +a && .venv/bin/python scripts/load_gold_to_snowflake.py $(YEAR) $(MONTH)'

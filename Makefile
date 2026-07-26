.PHONY: up up-streaming down clean init-data lint test dbt-debug dbt-build dbt-test soda-scan

# Load only the Snowflake/dbt vars from .env (the AIRFLOW_CONN_* URIs contain
# '&' and break shell sourcing); shared by the dbt-* targets below.
DBT_ENV = set -a && source <(grep -E "^(SNOWFLAKE_|DBT_SCHEMA)=" .env) && set +a

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

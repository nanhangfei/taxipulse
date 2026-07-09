.PHONY: up down init-data lint test dbt-build soda-scan

up:
	docker compose up -d --build

down:
	docker compose down -v

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

dbt-build:
	cd dbt_project && dbt deps && dbt build --target dev

soda-scan:
	soda scan -d snowflake_dev -c soda/configuration.yml soda/checks/

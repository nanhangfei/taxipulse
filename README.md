# TaxiPulse

Lambda-architecture data platform on the NYC TLC trip dataset — batch +
streaming with reconciliation, built to production conventions: contracts,
quarantine, idempotent backfills, lineage, monitoring, and CI.

## Stack
MinIO · Kafka + Schema Registry · Airflow · Spark · Flink (PyFlink) ·
SQL (Snowflake SQL / Flink SQL / dbt models) · Snowflake · dbt (+tests) ·
Soda · Prometheus/Grafana · OpenLineage/Marquez ·
Docker Compose · GitHub Actions · Metabase (Stage 6) · Power BI (design docs)

### Versions

| Component | Version | Pinned in |
|---|---|---|
| Python | 3.11 | `producer/Dockerfile`, CI |
| Apache Airflow | 3.3.0 (python3.11 image) | `docker-compose.yml`, CI |
| Apache Spark / PySpark | 3.5.9 (official `apache/spark` image; standalone master/worker via `spark-class`) | `docker-compose.yml`, CI |
| Apache Flink | 2.3.0 (java 17, Scala-free) | `docker-compose.yml` |
| Kafka (Confluent Platform) | cp 8.3.0 (≈ Kafka 4.x, KRaft-only) | `docker-compose.yml` |
| Schema Registry | cp 8.3.0 | `docker-compose.yml` |
| MinIO / mc | RELEASE.2025-09-07 / 2025-08-13 (community edition archived Apr 2026 — last releases) | `docker-compose.yml` |
| PostgreSQL (Airflow + Marquez) | 17 (newest in Airflow 3.3 / Marquez supported range; 18 is not yet) | `docker-compose.yml` |
| Marquez (+ web UI) | 0.50.0 | `docker-compose.yml` |
| Prometheus | v3.13.1 | `docker-compose.yml` |
| statsd-exporter | v0.29.0 (v0.30.0 has no Docker Hub image yet) | `docker-compose.yml` |
| Grafana | 13.1.1 | `docker-compose.yml` |
| Vault | 1.20.4 | `docker-compose.yml` |
| dbt (dbt-snowflake) | 1.12.0 (pulls dbt-core 1.12) | CI (`.github/workflows/ci.yml`) |
| dbt_utils package | 1.4.1 | `dbt_project/packages.yml` |
| confluent-kafka (Python, +avro) | 2.4.0 | `producer/requirements.txt` |
| pyarrow / pandas | 16.1.0 / 2.2.2 | `producer/requirements.txt` |
| Soda Core | *not pinned yet* | — (installed ad hoc by `make soda-scan`) |
| Snowflake | SaaS (no version) | — |

Floating tags to be aware of: `postgres:17` tracks the latest patch release;
Soda has no pin at all. Tightening these is part of Stage 5 (pinned-versions
review).

**Processing split:** Spark (DataFrame API) handles lake-layer heavy lifting —
parsing, validation, quarantine, partitioned writes. Once data reaches the
warehouse, all modeling is SQL: dbt models on Snowflake, DELETE+COPY loads via
SQLExecuteQueryOperator, and quality checks (dbt tests / Soda) compile to SQL. Even
the streaming job is written in Flink SQL, not the DataStream API.

## Architecture
```
TLC parquet ─► Spark ─► MinIO bronze/silver/gold ─► Snowflake ─► dbt ─► marts ─► Metabase
                                    ▲                              │
Replay producer ─► Kafka(Avro+SR) ─► Flink ─► realtime/ ─► RT_DB ──┴─► reconciliation DAG
```
- **Why Lambda, not Kappa:** the source is fundamentally batch; the replay
  producer exists to create a realistic streaming workload (event-time skew,
  bursts, late events). The reconciliation DAG measures batch/speed drift.

## Production behaviors implemented
| Concern | Where |
|---|---|
| Secrets | .env (ignored) + env_var() in dbt/Soda + GH Actions Secrets + Vault (stretch) |
| Contracts | Avro + Schema Registry; producer cannot violate schema |
| Bad data | Spark quarantine bucket with reject reasons; Kafka DLQ/late topics |
| Idempotency | Spark dynamic partition overwrite; DELETE+COPY per month; dbt merge |
| Backfill | `airflow dags backfill -s 2024-01-01 -e 2024-03-01 monthly_batch_pipeline` |
| Quality | dbt tests (build gate) + Soda (freshness/volume/distribution, runtime gate) |
| Lineage | OpenLineage from Airflow & Spark → Marquez UI |
| Monitoring | StatsD → Prometheus → Grafana; Slack on_failure_callback |
| Recon | `lambda_reconciliation` DAG: speed vs batch counts within 1% |
| Warehouse setup | `snowflake/setup.sql`: DBs, warehouses (auto-suspend 60s), RBAC, resource monitor |
| FinOps | warehouse-per-workload, monthly credit cap, transient staging schema |
| CI | ruff, pytest, DAG import test, dbt build in isolated schema |

## BI layer
The runnable dashboards are **Metabase** (added in Stage 6): a cached batch
dashboard on MARTS and a live one on RT_DB. `powerbi/DESIGN.md` is the
tool-agnostic dashboard design plus its enterprise Power BI translation —
star-schema mapping, DAX measures, Import (MARTS) vs DirectQuery (RT_DB)
rationale.

## Build roadmap

The platform is built in stages. Each stage has an exit criterion that must be
demonstrably true (not just "code exists") before moving on; work lands as one
or more commits per stage.

### Stage 0 — Foundations: local stack boots
**Done when:** `make up` brings the stack to healthy and every UI in the
checklist below is reachable.

#### 1. Initial commit

```bash
git add -A
git commit -m "Initial commit: lambda-architecture NYC taxi platform scaffold"
```

#### 2. Create `.env` from the example

```bash
cp .env.example .env
```

Then edit `.env`:

- Replace every `change-me-*` value with a real password
  (`openssl rand -hex 16` generates decent local-dev ones).
- Generate the Fernet key (Airflow encrypts stored credentials with it) and
  paste it into `AIRFLOW_FERNET_KEY=`:
  ```bash
  python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
  ```
- **Gotcha:** `AIRFLOW_CONN_MINIO_S3` hardcodes `change-me-minio` inside the
  URI — update it to match your `MINIO_ROOT_PASSWORD`, or Spark/Airflow won't
  reach MinIO later.
- The Snowflake block and `SLACK_WEBHOOK_URL` can stay as placeholders —
  nothing in Stage 0 touches them (Snowflake matters from Stage 1,
  Slack alerts at Stage 4).

#### 3. Download the data

```bash
make init-data
ls -lh data/   # expect two files with real sizes (~48 MB parquet + small CSV)
```

#### 4. Bring the stack up and verify

Prerequisite: Docker Desktop running with **at least 8 GB memory allocated**
(Settings → Resources). The stack runs ~16 containers (streaming services —
Kafka, Schema Registry, the replay producer, and Flink — are behind a compose
`streaming` profile and don't start until Stage 7, since nothing produces or
consumes from them before then); the default 2 GB allocation causes confusing
OOM kills.

```bash
make up            # first run builds images — takes a while
docker compose ps  # verify STATUS column; re-check after 2 min (crash loops briefly show Up)
```

Verification checklist:
- [x] All containers `Up` / `healthy` in `docker compose ps`, and stay that way
- [x] MinIO console at http://localhost:9001 shows bronze/silver/gold/quarantine
      buckets (auto-created by the `mc` init container)
- [x] Airflow UI at http://localhost:8080 (login `admin` / `AIRFLOW_ADMIN_PASSWORD`)
- [x] Spark master UI at http://localhost:8082 shows 1 worker registered
- [x] Grafana :3001 · Marquez :3000 · Prometheus :9090 reachable
      (Kafka :9092, Schema Registry :8081, and Flink :8083 only appear under
      `make up-streaming`, Stage 7)

### Stage 1 — Warehouse & modeling: Snowflake + dbt
This stage needs only a Snowflake account — no Docker services. Until the
lake path exists (Stage 2), raw data is loaded into Snowflake directly; dbt
models read it through a `source()`, so re-pointing them at the gold-fed
table later is close to a config-only change.

> **Adapted setup:** this build connects to an existing Snowflake account with
> **key-pair auth** (Snowflake now blocks single-factor password logins) and
> reuses its objects — database `analytics`, schema `raw`, role `transformer`,
> warehouse `transforming`. The production-RBAC design in `snowflake/setup.sql`
> (`RAW_DB`/`ANALYTICS_DB`, per-workload warehouses, least-privilege service
> users, monthly resource monitor) is kept as a reference but not run here.
> dbt materializes into `dbt_staging` / `dbt_marts` (profile schema + model schema).

Run dbt via the Makefile wrappers from the repo root — they load the Snowflake
vars from `.env`, use the project venv, and pass `--profiles-dir .`:
`make dbt-debug` (connection check), `make dbt-build` (deps + build),
`make dbt-test` (tests only).

- [x] dbt installed locally in its own venv: `pip install
      dbt-snowflake==1.12.0` (same pin as CI; it is not in the Airflow
      image until Stage 3)
- [x] dbt connects to Snowflake via **key-pair**: fill the `SNOWFLAKE_*` block
      in `.env` (`SNOWFLAKE_PRIVATE_KEY_PATH` points at the PEM/p8 key, which
      stays out of the repo), load it (`set -a; source .env; set +a`), then
      `dbt debug --profiles-dir .` from `dbt_project/` is green — committed
      `profiles.yml` reads only env vars, no secrets in git
- [x] Raw January parquet loaded into `analytics.raw.fct_trips_raw`, normalized
      to the schema `stg_trips` expects: rename/cast raw TLC columns
      (`tpep_pickup_datetime` → `pickup_datetime`, `PULocationID` →
      `pickup_zone_id`, …) and add `year`, `month`, `_ingested_at`. Loaded via
      pandas `write_pandas` (PUT + COPY under the hood) with
      `use_logical_type=True` so timestamps land as TIMESTAMP, not epoch NUMBER
- [x] `taxi_zone_lookup.csv` loaded into `analytics.raw.zone_lookup` with quoted,
      case-sensitive columns — `dim_zones` reads `"LocationID"` etc., so casing
      must be preserved
- [x] Staging filters out invalid rows (negative fares, zero/negative duration,
      bad payment types) and dedups the trip grain so tests pass — the raw file
      contains them; from Stage 2 the lake path delivers pre-cleaned data and
      these filters become harmless no-ops
- [x] dbt: `stg_trips` → `fct_trips` (incremental merge on `trip_sk`) /
      `dim_zones` (table) / `dim_date` (table, built from a `dbt_utils` spine)
- [x] `dbt build` green: all models materialize and all 11 tests pass
      (incl. `unique` on both dimension keys)
- [x] Loads are idempotent: re-running the pandas load (`overwrite=True`) +
      `dbt build` changes nothing — a second/third build is a no-op, counts hold
      (fct_trips 2,788,205 · dim_zones 265 · dim_date 366)

**Done when:** `dbt build` succeeds against Snowflake on the directly-loaded
raw data, and a second load + build run is a no-op. ✓

### Stage 2 — Batch lake path: Spark bronze → silver → gold
Spark talks to MinIO over `s3a://`, which needs AWS jars the official
`apache/spark` image doesn't ship — so `spark_jobs/Dockerfile` layers
`hadoop-aws` + `aws-java-sdk-bundle` (pinned to the image's Hadoop 3.3.4) onto
it. Jobs and `data/` are mounted (not baked) into both master and worker.

- [x] `ingest_bronze.py` lands raw parquet in MinIO bronze, partitioned by month
- [x] `transform_silver.py` cleans/validates; rejects go to quarantine **with
      reject reasons**, never dropped (Jan 2024: 38,345 quarantined — negative
      fare 37,444 / zero-or-negative duration 870 / absurd distance 31)
- [x] `load_gold.py` writes the trip fact + daily-zone aggregates with dynamic
      partition overwrite
- [x] Re-running the same month is idempotent: dynamic partition overwrite on
      the lake, DELETE-then-load per month into Snowflake, dbt incremental merge
      — a second full run changes no counts (gold 2,926,279 · fct_trips 2,788,194)
- [x] Gold → Snowflake via `scripts/load_gold_to_snowflake.py`: pulls gold
      parquet from MinIO (pyarrow) and pushes it with the connector's PUT+COPY
      (`write_pandas`) into `analytics.raw.fct_trips_gold` — the internal-stage
      pattern from Stage 1, idempotent via DELETE per month
- [x] dbt `source()` re-pointed from the Stage 1 raw table (`fct_trips_raw`) to
      the gold-fed table (`fct_trips_gold`); `dbt build` green, staging filters
      now no-ops because silver already cleaned the data

**Done when:** one month flows raw → gold by hand-running the three jobs, a
deliberately corrupted record shows up in quarantine with its reason, and
`dbt build` is green on top of the gold-fed source. ✓

**Running it** — all commands are run from the repo root.

```bash
# 0. one-time prerequisites
make init-data                  # download Jan 2024 parquet + zone lookup (if not already in data/)
docker compose build spark-master   # build the custom Spark image (S3A jars) — needed once

# 1. start the batch stack (MinIO, Spark, ...)
make up                         # or just: docker compose up -d minio minio-init spark-master spark-worker

# 2. run the lake chain (bronze -> silver -> gold)
make spark-batch                # all three for Jan 2024
#   or step-by-step to inspect between stages:
#   make spark-bronze           # local parquet -> s3a://bronze
#   make spark-silver           # bronze -> s3a://silver (+ quarantine)
#   make spark-gold             # silver -> s3a://gold

# 3. load gold into Snowflake, then model with dbt
make snowflake-load-gold        # s3a://gold/fct_trips -> analytics.raw.fct_trips_gold
make dbt-build                  # staging -> marts + tests
```

Every job target takes `YEAR`/`MONTH`/`RAW` overrides to process another month
(`RAW` is the path *inside* the container — `./data` is mounted at `/opt/data`,
so download the file into `data/` first):

```bash
make spark-batch YEAR=2024 MONTH=2 RAW=/opt/data/yellow_tripdata_2024-02.parquet
make snowflake-load-gold YEAR=2024 MONTH=2
make dbt-build
```

Watch it run: **Spark UI** at `http://localhost:8082` (job progress) and the
**MinIO console** at `http://localhost:9001` (buckets filling).

### Stage 3 — Orchestration: Airflow owns the batch path
- [ ] `monthly_batch_pipeline` DAG runs Stage 2 end-to-end on a schedule
      (Spark jobs + Snowflake load + dbt build)
- [ ] Backfill works: `airflow dags backfill -s 2024-01-01 -e 2024-03-01`
      loads three months without duplicates (`make init-data` only downloads
      January — fetch the 2024-02/03 parquet files first)
- [ ] Failure of any task is visible in the UI and retries sensibly

**Done when:** three months are loaded via backfill and a re-run of the whole
DAG changes nothing (row counts stable).

### Stage 4 — Quality gates & observability
- [ ] Soda runtime checks wired into Airflow: freshness, volume, distribution
      drift on `fct_trips` (the `rt_zone_demand` check joins in Stage 7 with
      the streaming path)
- [ ] OpenLineage events from Airflow and Spark visible as a full graph in
      Marquez
- [ ] Grafana `pipeline_health` dashboard shows live pipeline metrics;
      Prometheus scrapes all exporters
- [ ] Slack `on_failure_callback` fires on a forced failure

**Done when:** killing a service or feeding bad data produces a visible alert
and the lineage graph covers source → marts.

### Stage 5 — Testing & CI/CD
- [ ] pytest suite for Spark jobs (known TODO): transform logic, quarantine
      rules, idempotency — runnable locally and in CI
- [ ] CI green end-to-end: ruff, pytest, DAG import test, dbt build in an
      isolated CI schema (dropped afterwards)
- [ ] Pinned versions reviewed and bumped where needed

**Done when:** a PR cannot merge with a broken DAG, failing dbt test, or
failing Spark unit test.

### Stage 6 — Product polish: BI, docs, demo
- [ ] **Metabase** service added to docker-compose as the runnable BI layer
      (Power BI Desktop is Windows-only; this project is developed on macOS).
      Needs its own Postgres metadata DB; host port 3002 (3000 = Marquez-web).
- [ ] Cached batch dashboard on MARTS, built from the designs in
      `powerbi/DESIGN.md` (the live near-realtime dashboard on RT_DB joins
      in Stage 7 with the streaming path)
- [ ] `powerbi/DESIGN.md` kept as the tool-agnostic design doc + "how this
      maps to Power BI/DAX in an enterprise setting" translation
- [ ] Architecture docs finalized; screenshots in README
- [ ] Chaos pass (batch scope): replay a duplicate month, malform a record —
      document that quarantine and idempotency hold
- [ ] Cost review: warehouse auto-suspend and credit caps verified in practice

**Done when:** a stranger can clone the repo, follow the setup docs, and see
data flowing into a dashboard — the batch platform is a complete, demo-able
product on its own.

### Stage 7 — Streaming capstone: Kafka → Flink → realtime marts
The batch product ships first (Stages 1–6); the speed layer upgrades it to
full lambda architecture as a capstone. Kafka, Schema Registry, the topics,
the replay producer, and Flink all sit behind the compose `streaming`
profile — none of them run before this stage. Start everything with
`make up-streaming`.

- [ ] Replay producer emits Avro events through Schema Registry with realistic
      event-time skew and bursts; late/bad events go to DLQ/late topics
- [ ] Custom Flink image with Kafka/Avro connector JARs (known TODO)
- [ ] `zone_demand.py` computes 5-min zone counts into `realtime/` → RT_DB
- [ ] `lambda_reconciliation` DAG compares speed vs batch counts, alerts
      beyond 1% tolerance
- [ ] Soda checks extended to `rt_zone_demand`
- [ ] Live near-realtime Metabase dashboard on RT_DB
- [ ] Chaos pass (streaming scope): kill Kafka mid-replay — document that
      recovery and reconciliation hold

**Done when:** producer replays a day of data, Flink output lands in RT_DB,
the reconciliation DAG passes within tolerance, and the live dashboard moves
while the replay runs.

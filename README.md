# TaxiPulse

Lambda-architecture data platform on the NYC TLC trip dataset — batch +
streaming with reconciliation, built to production conventions: contracts,
quarantine, idempotent backfills, lineage, monitoring, CI, and IaC.

## Stack
MinIO · Kafka + Schema Registry · Airflow · Spark · Flink (PyFlink) ·
SQL (Snowflake SQL / Flink SQL / dbt models) · Snowflake · dbt (+tests) ·
Soda · Terraform · Prometheus/Grafana · OpenLineage/Marquez ·
Docker Compose · GitHub Actions · Power BI (design docs)

### Versions

| Component | Version | Pinned in |
|---|---|---|
| Python | 3.11 | `producer/Dockerfile`, CI |
| Apache Airflow | 2.9.3 (python3.11 image) | `docker-compose.yml`, CI |
| Apache Spark / PySpark | 3.5.1 (bitnami image) | `docker-compose.yml`, CI |
| Apache Flink | 1.19 (scala 2.12, java 11) | `docker-compose.yml` |
| Kafka (Confluent Platform) | cp 7.6.1 (≈ Kafka 3.6) | `docker-compose.yml` |
| Schema Registry | cp 7.6.1 | `docker-compose.yml` |
| MinIO / mc | RELEASE.2024-06-13 / 2024-06-12 | `docker-compose.yml` |
| PostgreSQL (Airflow + Marquez) | 15 | `docker-compose.yml` |
| Marquez (+ web UI) | 0.47.0 | `docker-compose.yml` |
| Prometheus | v2.53.0 | `docker-compose.yml` |
| statsd-exporter | v0.26.1 | `docker-compose.yml` |
| Grafana | 11.1.0 | `docker-compose.yml` |
| Vault | 1.17 | `docker-compose.yml` |
| dbt (dbt-snowflake) | 1.8.3 | CI (`.github/workflows/ci.yml`) |
| dbt_utils package | 1.2.0 | `dbt_project/packages.yml` |
| Terraform | >= 1.7 | `terraform/versions.tf` |
| Snowflake provider | ~> 0.94 (Snowflake-Labs) | `terraform/versions.tf` |
| confluent-kafka (Python, +avro) | 2.4.0 | `producer/requirements.txt` |
| pyarrow / pandas | 16.1.0 / 2.2.2 | `producer/requirements.txt` |
| Soda Core | *not pinned yet* | — (installed ad hoc by `make soda-scan`) |
| Snowflake | SaaS (no version) | — |

Floating tags to be aware of: `flink:1.19` and `postgres:15` track the latest
patch release, `vault:1.17` the latest minor patch; Soda has no pin at all.
Tightening these is part of Stage 6 (pinned-versions review).

**Processing split:** Spark (DataFrame API) handles lake-layer heavy lifting —
parsing, validation, quarantine, partitioned writes. Once data reaches the
warehouse, all modeling is SQL: dbt models on Snowflake, DELETE+COPY loads via
SnowflakeOperator, and quality checks (dbt tests / Soda) compile to SQL. Even
the streaming job is written in Flink SQL, not the DataStream API.

## Architecture
```
TLC parquet ─► Spark ─► MinIO bronze/silver/gold ─► Snowflake ─► dbt ─► marts ─► Power BI
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
| IaC | Terraform: DBs, warehouses (auto-suspend 60s), RBAC, resource monitor |
| FinOps | warehouse-per-workload, monthly credit cap, transient staging schema |
| CI | ruff, pytest, DAG import test, dbt build in isolated schema, tf validate |

## Power BI (design docs)
`powerbi/` — star-schema mapping, DAX measure definitions, Import vs
DirectQuery rationale, dashboard screenshots. Batch dashboard = Import on
MARTS; near-realtime dashboard = DirectQuery on RT_DB.

## Build roadmap

The platform is built in stages. Each stage has an exit criterion that must be
demonstrably true (not just "code exists") before moving on; work lands as one
or more commits per stage.

### Stage 0 — Foundations: local stack boots
- [ ] Git repository with initial commit; `.env` from `.env.example`
- [ ] `make init-data` downloads one month of TLC parquet
- [ ] Core services healthy under Docker Compose: MinIO, Kafka, Schema
      Registry, Postgres, Airflow webserver/scheduler
- [ ] MinIO buckets (bronze/silver/gold/quarantine) auto-created

**Done when:** `make up` brings the stack to healthy and every UI in the
walkthrough checklist below is reachable.

<details>
<summary><b>Stage 0 walkthrough</b> (step-by-step)</summary>

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
  nothing in Stage 0 touches them (they matter in Stages 3 and 5).

#### 3. Download the data

```bash
make init-data
ls -lh data/   # expect two files with real sizes (~48 MB parquet + small CSV)
```

#### 4. Bring the stack up and verify

Prerequisite: Docker Desktop running with **at least 8 GB memory allocated**
(Settings → Resources). The stack runs ~12 containers; the default 2 GB
allocation causes confusing OOM kills.

```bash
make up            # first run builds images — takes a while
docker compose ps  # verify STATUS column; re-check after 2 min (crash loops briefly show Up)
```

Verification checklist:
- [ ] All containers `Up` / `healthy` in `docker compose ps`, and stay that way
- [ ] Airflow UI at http://localhost:8080 (login `admin` / `AIRFLOW_ADMIN_PASSWORD`)
- [ ] MinIO console at http://localhost:9001 shows bronze/silver/gold/quarantine
      buckets (auto-created by the `mc` init container)
- [ ] Schema Registry http://localhost:8081/subjects returns `[]`
- [ ] Grafana :3001 · Marquez :3000 · Prometheus :9090 · Flink :8083 reachable

#### Common failure modes

| Symptom | Likely cause / fix |
|---|---|
| Container fails to bind port | Something local already uses 8080/3000/9090 — `lsof -i :8080` |
| Airflow UI 502s | Init container not finished — `docker compose logs airflow-init` |
| Flink unhealthy | Missing Kafka/Avro connector JARs — known TODO, fixed in Stage 4; for Stage 0 it only needs to start |
| Containers randomly dying | Docker memory allocation too low |

</details>

### Stage 1 — Batch lake path: Spark bronze → silver → gold
- [ ] `ingest_bronze.py` lands raw parquet in MinIO bronze, partitioned by month
- [ ] `transform_silver.py` cleans/validates; rejects go to quarantine **with
      reject reasons**, never dropped
- [ ] `load_gold.py` writes aggregates with dynamic partition overwrite
- [ ] Re-running the same month twice produces identical output (idempotency
      proven, not assumed)

**Done when:** one month flows raw → gold by hand-running the three jobs, and a
deliberately corrupted record shows up in quarantine with its reason.

### Stage 2 — Orchestration: Airflow owns the batch path
- [ ] `monthly_batch_pipeline` DAG runs Stage 1 end-to-end on a schedule
- [ ] Backfill works: `airflow dags backfill -s 2024-01-01 -e 2024-03-01`
      loads three months without duplicates
- [ ] Failure of any task is visible in the UI and retries sensibly

**Done when:** three months are loaded via backfill and a re-run of the whole
DAG changes nothing (row counts stable).

### Stage 3 — Warehouse & modeling: Snowflake + dbt
- [ ] Terraform applies cleanly: databases, warehouses (auto-suspend), RBAC,
      resource monitor
- [ ] Gold → Snowflake load path works (`CREATE STAGE` on MinIO, or switch to
      real S3 — currently a known TODO)
- [ ] dbt: `stg_trips` → `fct_trips`/`dim_zones`, plus the missing `dim_date`
- [ ] `dbt build` green: all models materialize and all tests pass
- [ ] Loads are idempotent (DELETE+COPY per month; dbt merge)

**Done when:** `dbt build` succeeds against Snowflake and marts row counts
reconcile with gold-layer counts.

### Stage 4 — Streaming path: Kafka → Flink → realtime marts
- [ ] Replay producer emits Avro events through Schema Registry with realistic
      event-time skew and bursts; late/bad events go to DLQ/late topics
- [ ] Custom Flink image with Kafka/Avro connector JARs (known TODO)
- [ ] `zone_demand.py` computes 5-min zone counts into `realtime/` → RT_DB
- [ ] `lambda_reconciliation` DAG compares speed vs batch counts, alerts
      beyond 1% tolerance

**Done when:** producer replays a day of data, Flink output lands in RT_DB,
and the reconciliation DAG passes within tolerance.

### Stage 5 — Quality gates & observability
- [ ] Soda runtime checks wired into Airflow: freshness, volume, distribution
      drift on `fct_trips` and `rt_zone_demand`
- [ ] OpenLineage events from Airflow and Spark visible as a full graph in
      Marquez
- [ ] Grafana `pipeline_health` dashboard shows live pipeline metrics;
      Prometheus scrapes all exporters
- [ ] Slack `on_failure_callback` fires on a forced failure

**Done when:** killing a service or feeding bad data produces a visible alert
and the lineage graph covers source → marts.

### Stage 6 — Testing & CI/CD
- [ ] pytest suite for Spark jobs (known TODO): transform logic, quarantine
      rules, idempotency — runnable locally and in CI
- [ ] CI green end-to-end: ruff, pytest, DAG import test, dbt build in an
      isolated CI schema (dropped afterwards), `terraform validate`
- [ ] Pinned versions reviewed and bumped where needed

**Done when:** a PR cannot merge with a broken DAG, failing dbt test, or
failing Spark unit test.

### Stage 7 — Product polish: BI, docs, demo
- [ ] Power BI dashboards built from `powerbi/DESIGN.md`: Import-mode batch
      dashboard on MARTS, DirectQuery near-realtime dashboard on RT_DB
- [ ] Architecture docs finalized; screenshots in README
- [ ] Chaos pass: kill Kafka mid-replay, replay a duplicate month, malform a
      record — document that quarantine, idempotency, and reconciliation hold
- [ ] Cost review: warehouse auto-suspend and credit caps verified in practice

**Done when:** a stranger can clone the repo, follow Quick start, and see data
flowing into dashboards — and the failure-mode story is documented, not just
claimed.

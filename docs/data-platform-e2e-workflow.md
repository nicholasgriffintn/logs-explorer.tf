# End-to-end data and ML workflow

This document defines the Airflow operational flow across ingest, Spark processing, Trino querying, and ML.

## 1) Ingest raw data continuously

Run ingest service:

```bash
pnpm --filter @logs-explorer/ingest-service dev
```

Data lands in:

- `tf2.default.logs`
- `tf2.default.summaries`
- `tf2.default.messages`

## 2) Start Trino runtime

```bash
docker compose -f infra/trino/docker-compose.yml up -d
```

## 3) Start Airflow

```bash
pnpm airflow:up
```

```bash
pnpm airflow:dags
```

## 4) Run and schedule through DAGs

You should be able to access the Airflow UI at `http://localhost:8080` with credentials from `infra/airflow/airflow.env`.

Use these DAGs as the operating model:

- `tf2_platform_e2e_daily`
- `tf2_feature_serving_daily`
- `tf2_ml_daily_or_weekly`
- `tf2_iceberg_maintenance_weekly`
- `tf2_backfill_manual`

## 5) Manual execution examples

Trigger full E2E now:

```bash
pnpm airflow:trigger:e2e
```

Trigger E2E with training and maintenance:

```bash
infra/airflow/scripts/airflow.sh trigger-e2e '{"run_baseline_training": true, "model_version": "v1.2.0", "run_iceberg_maintenance": true}'
```

Trigger manual backfill:

```bash
infra/airflow/scripts/airflow.sh trigger tf2_backfill_manual '{"mode": "full", "pipeline": "all"}'
```

## 6) Query and dashboards

Trino serves read workloads on top of Airflow-orchestrated Spark outputs.

- Query pack: `infra/trino/queries/README.md`
- Superset setup: `infra/superset/README.md`

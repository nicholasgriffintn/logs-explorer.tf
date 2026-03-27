# Airflow orchestration

Airflow is the orchestration entrypoint for TF2 platform refresh operations. All scheduled and manual processing runs through Airflow DAGs.

## Architecture

- `postgres`: Airflow metadata database
- `redis`: Celery broker
- `airflow-api-server`: UI and API (`http://localhost:8080`)
- `airflow-scheduler`: scheduling
- `airflow-dag-processor`: DAG parsing
- `airflow-worker`: task execution
- `airflow-triggerer`: deferrable task trigger loop
- `airflow-init`: DB migration, admin user bootstrap, Spark/Trino connection bootstrap

## Operator model

DAGs use native Airflow providers:

- `SparkSubmitOperator` for Spark processing refreshes
- `SQLExecuteQueryOperator` for Trino quality/readiness/maintenance SQL
- `PythonOperator` for DAG control logic and ML baseline training execution

## OpenLineage

OpenLineage is wired into Airflow and Spark submit tasks.

- Airflow provider: `apache-airflow-providers-openlineage` (installed in `infra/airflow/Dockerfile`)
- Spark listener: `io.openlineage:openlineage-spark_2.12` (injected via DAG `packages`)
- Parent linkage: enabled through Airflow OpenLineage Spark injection flags

Configure in `infra/airflow/airflow.env`:

- `OPENLINEAGE_TRANSPORT` (JSON string; required for event delivery)
- `OPENLINEAGE_DISABLED` (`false` by default)
- `OPENLINEAGE_NAMESPACE` (Airflow job namespace)
- `OPENLINEAGE_SPARK_NAMESPACE` (Spark job namespace)
- `OPENLINEAGE_SPARK_PACKAGE_VERSION` (default `1.36.0`)

Example transport value:

```bash
OPENLINEAGE_TRANSPORT={"type":"http","url":"http://marquez:5000","endpoint":"api/v1/lineage"}
OPENLINEAGE_DISABLED=false
```

## Prerequisites

- Docker Desktop or Docker Engine with Compose v2
- `infra/spark/spark.env` configured
- Trino runtime started (`docker compose -f infra/trino/docker-compose.yml up -d`)

## Quick start

1. Prepare config:

```bash
cp infra/airflow/airflow.env.example infra/airflow/airflow.env
```

2. Start OpenLineage backend:

```bash
pnpm openlineage:up
```

3. Start Airflow:

```bash
pnpm airflow:up
```

`airflow-init` bootstraps Airflow Variables from `infra/spark/spark.env` + `infra/airflow/airflow.env`, and creates required Airflow Connections.

4. Verify DAG registration:

```bash
pnpm airflow:dags
```

5. Open UI:

- `http://localhost:8080`
- Username/password from `infra/airflow/airflow.env`

## Core DAGs

- `tf2_platform_e2e_daily`: feature refresh, quality gate, ML refresh, readiness gate, optional training, optional maintenance
- `tf2_feature_serving_daily`: feature refresh + serving quality gate
- `tf2_ml_daily_or_weekly`: ML refresh + readiness gate + optional baseline training
- `tf2_iceberg_maintenance_weekly`: Iceberg optimise/expire operations
- `tf2_backfill_manual`: full/incremental backfill with pipeline slice selection

## Trigger commands

Trigger E2E:

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

## Operations

Status:

```bash
pnpm airflow:status
```

Logs:

```bash
infra/airflow/scripts/airflow.sh logs
```

Stop:

```bash
pnpm airflow:down
```

If you previously started the stack with a different container user and hit init/import errors, reset volumes once:

```bash
docker compose -f infra/airflow/docker-compose.yml down -v
pnpm airflow:up
```

## Version pinning

Airflow image pin is controlled by `AIRFLOW_BASE_IMAGE` in `infra/airflow/airflow.env`.
Default is `apache/airflow:3.1.8-python3.12`.

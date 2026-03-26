# Refresh operations runbook

This runbook defines Airflow refresh operations and recovery.

## Prerequisites

- `infra/spark/spark.env` configured
- Trino running (`infra/trino/docker-compose.yml`)
- Airflow running (`pnpm airflow:up`)

## Operational entrypoints

Default DAGs:

- `tf2_platform_e2e_daily`
- `tf2_feature_serving_daily`
- `tf2_ml_daily_or_weekly`
- `tf2_iceberg_maintenance_weekly`
- `tf2_backfill_manual`

Trigger full E2E:

```bash
pnpm airflow:trigger:e2e
```

Trigger manual backfill:

```bash
infra/airflow/scripts/airflow.sh trigger tf2_backfill_manual '{"mode": "full", "pipeline": "all"}'
```

## Execution contract

- Spark processing is submitted by `SparkSubmitOperator`.
- Quality/readiness/maintenance SQL is executed by `SQLExecuteQueryOperator` against Trino.
- ML baseline training is executed by Airflow task runtime with explicit env contract.

## Run metadata

Spark writes step-level and pipeline-level status rows to:

- `tf2.default.ops_pipeline_runs`

Useful query:

```sql
SELECT
  run_id,
  run_mode,
  step_name,
  status,
  started_at,
  finished_at,
  duration_seconds,
  row_count,
  error_text
FROM tf2.default.ops_pipeline_runs
ORDER BY started_at DESC
LIMIT 50;
```

## Recovery workflow

- Open the failed DAG run in Airflow UI.
- Inspect failed task logs and error output.
- Fix root cause (data, schema, config, or runtime).
- Clear and rerun failed tasks from Airflow.
- If repeated incremental failures indicate drift, trigger `tf2_backfill_manual` with full mode.

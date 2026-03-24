# Refresh operations runbook

This runbook defines Spark processing operations and recovery.
Feature-serving and ML are separate pipelines and can run on different schedules.
For the full platform flow, see `docs/data-platform-e2e-workflow.md`.

## Prerequisites

- Spark config exists at `infra/spark/spark.env` (or equivalent env exports).
- Docker is running and can access the target network.

## Pipeline entrypoints

Feature-serving:

```bash
infra/spark/run_feature_pipeline.sh incremental
```

```bash
infra/spark/run_feature_pipeline.sh full
```

ML:

```bash
infra/spark/run_ml_pipeline.sh incremental
```

```bash
infra/spark/run_ml_pipeline.sh full
```

Combined (optional):

```bash
infra/spark/run_processing_pipeline.sh incremental all
```

Optional environment overrides:

- `SPARK_ENV_FILE`: config file path (default empty; env vars can be exported directly)
- `SPARK_NETWORK`: Docker network name (default `logs-explorer`)
- `SPARK_IMAGE`: image tag (default `logs-explorer-spark-processing:latest`)
- `REFRESH_DAYS`: rolling window for incremental mode (default `7`)

## Execution contract

- Spark owns all refresh/materialisation for `features_*`, `serving_*`, and ML tables.
- Trino is query and dashboard serving only.

## Recommended cadence

- Feature-serving pipeline: frequent (hourly/daily).
- ML pipeline: separate cadence (daily/weekly or triggered by model lifecycle windows).

## Quality gate

Run serving quality checks after feature-serving pipeline execution:

```bash
docker exec -i tf2-trino trino < infra/trino/queries/19_data_quality_checks.sql
```

Run ML readiness checks after ML pipeline execution:

```bash
infra/trino/queries/run_ml_readiness_check.sh
```

## Run metadata

Spark writes step-level and pipeline-level status rows to:

- `tf2.default.ops_pipeline_runs`

Pipeline rows use step names:

- `pipeline_feature_serving`
- `pipeline_ml`
- `pipeline_all` (combined runs)

Useful queries:

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

```sql
SELECT
  run_id,
  MAX(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS has_failure,
  MIN(started_at) AS pipeline_started_at,
  MAX(finished_at) AS pipeline_finished_at
FROM tf2.default.ops_pipeline_runs
WHERE step_name LIKE 'pipeline_%'
GROUP BY run_id
ORDER BY pipeline_started_at DESC
LIMIT 20;
```

## Recovery playbook

- If incremental fails, inspect `ops_pipeline_runs.error_text`, fix root cause, rerun incremental.
- If failures persist or schema drift is suspected, run `full` once, then rerun incremental.
- If quality checks fail, do not publish dashboard updates; repair data issues and rerun.

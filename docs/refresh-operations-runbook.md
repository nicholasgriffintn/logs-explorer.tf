# Refresh operations runbook

This runbook defines how we execute and recover the `core -> features -> serving` refresh pipeline.

## Prerequisites

- Trino container is running:

```bash
docker compose -f infra/trino/docker-compose.yml up -d
```

- `tf2` catalog is configured (`infra/trino/catalog/tf2.properties`).

## One command entrypoint

Use the runner script for both full and incremental refreshes:

```bash
infra/trino/queries/run_refresh_pipeline.sh incremental
```

```bash
infra/trino/queries/run_refresh_pipeline.sh full
```

Optional environment overrides:

- `TRINO_CONTAINER`: non-default container name (default `tf2-trino`)
- `RUN_ID`: explicit run identifier

## Mode selection

- `incremental` (default): rewrites a rolling 7-day window in `features_player_match`, `serving_map_overview_daily`, and `serving_player_match_deep_dive`; recomputes full history for changed players in `features_player_recent_form` and `serving_player_profiles`; materialises the latest training snapshot (`25`); refreshes ML serving progress tables.
- `full`: drops and rebuilds feature/serving tables from full core history using scripts `11` to `14`, `25`, `27`, and `29` (plus model registry table guard from `26`).

Use `full` for first-time setup, backfills, and schema-repair events.
Use `incremental` for routine daily refreshes.

## Quality gate

The runner executes `19_data_quality_checks.sql` after refresh.
If any check returns `FAIL`, the runner exits non-zero and records failure metadata.

For ML-specific readiness checks, run:

```bash
infra/trino/queries/run_ml_readiness_check.sh
```

To run baseline training from the latest snapshot and register candidate versions:

```bash
MODEL_VERSION=v1.0.0 infra/trino/queries/run_ml_baseline_training.sh
```

The command builds and runs the containerised trainer and updates:

- `artifacts/ml/...` model files
- `docs/ml-offline-evaluation-report.md`
- `tf2.default.ml_model_registry` candidate rows

## Run metadata

The runner writes step-level and pipeline-level status rows to:

- `tf2.default.ops_pipeline_runs`

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
GROUP BY run_id
ORDER BY pipeline_started_at DESC
LIMIT 20;
```

## Recovery playbook

- If incremental fails due to SQL/data issues, inspect `ops_pipeline_runs.error_text`, run the failed SQL script directly, then rerun incremental after fixing.
- If failures persist or schemas drift, run `full` mode once, then rerun incremental to verify steady-state behaviour.
- If quality checks fail, do not publish dashboard data; inspect failed rows from `19_data_quality_checks.sql`, repair, then rerun.

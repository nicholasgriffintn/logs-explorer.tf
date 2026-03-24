# End-to-end data and ML workflow

This document defines the operational flow across ingest, Spark processing, Trino querying, and ML.
Feature-serving and ML are separate processes and should be scheduled independently.

## 1) Ingest raw data continuously

Run ingest service:

```bash
pnpm --filter @logs-explorer/ingest-service dev
```

Data lands in:

- `tf2.default.logs`
- `tf2.default.summaries`
- `tf2.default.messages`

## 2) Start Trino query runtime

Start Trino and keep it running for checks, ad-hoc queries, ML readiness checks, and Superset:

```bash
docker compose -f infra/trino/docker-compose.yml up -d
```

## 3) Refresh feature-serving data (frequent)

Run:

```bash
infra/spark/run_feature_pipeline.sh incremental
```

Use `full` for first build/backfill:

```bash
infra/spark/run_feature_pipeline.sh full
```

Outputs:

- `features_player_match`
- `features_player_recent_form`
- `serving_player_profiles`
- `serving_map_overview_daily`
- `serving_player_match_deep_dive`

## 4) Refresh ML data (separate cadence)

Run:

```bash
infra/spark/run_ml_pipeline.sh incremental
```

Use `full` for first build/backfill:

```bash
infra/spark/run_ml_pipeline.sh full
```

Outputs:

- `ml_training_dataset_snapshots`
- `ml_training_player_match`
- `ml_model_registry`
- `ml_model_stage_history`
- `ml_model_validation_metrics_daily`
- `serving_ml_model_registry`
- `serving_ml_pipeline_progress_daily`
- `serving_ml_prediction_quality_daily`

## 5) Run quality checks and training

Serving quality checks:

```bash
docker exec -i tf2-trino trino < infra/trino/queries/quality/data_quality_checks.sql
```

Readiness checks:

```bash
infra/trino/queries/ml/run_ml_readiness_check.sh
```

Baseline training:

```bash
MODEL_VERSION=v1.0.0 infra/trino/queries/ml/run_ml_baseline_training.sh
```

## 6) Query and dashboards (Trino/Superset)

Trino serves query workloads on top of Spark-produced tables.

- Query pack: `infra/trino/queries/README.md`
- Superset setup: `infra/superset/README.md`

## Suggested schedule

- Ingest: continuous.
- Feature-serving pipeline: hourly/daily.
- ML pipeline: daily/weekly, aligned to retraining windows.
- Baseline/production training: triggered by model lifecycle policy.
- Iceberg maintenance (`infra/trino/queries/ops/run_iceberg_maintenance.sh`): weekly (or more often if ingest volume increases).

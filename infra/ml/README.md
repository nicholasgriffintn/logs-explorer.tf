# ML training runtime

This directory contains the containerised ML training runtime for baseline models.

## Why containerised

- keeps Python/ML dependencies isolated from host setup
- makes training runs reproducible across machines
- reduces local environment drift and maintenance overhead

## Image build + run

Use the query runner entrypoint:

```bash
MODEL_VERSION=v1.0.0 infra/trino/queries/ml/run_ml_baseline_training.sh
```

This will:

- build `infra/ml/Dockerfile` into `logs-explorer-ml-trainer:latest` (override with `ML_TRAINER_IMAGE`)
- run the trainer on the `logs-explorer` Docker network (override with `ML_TRAINER_NETWORK`)
- connect to Trino (`tf2-trino:8080` by default)
- write artefacts under `artifacts/ml/...`
- write `docs/ml-offline-evaluation-report.md`
- upsert candidate rows into `tf2.default.ml_model_registry`
- upsert daily validation quality rows into `tf2.default.ml_model_validation_metrics_daily`

## Useful overrides

- `TRINO_HOST`, `TRINO_PORT`, `TRINO_USER`, `TRINO_CATALOG`, `TRINO_SCHEMA`
- `MODEL_VERSION`
- `SNAPSHOT_ID` (optional pin; default is latest)
- `TRAIN_RATIO`
- `TRAINING_CODE_VERSION`, `FEATURE_SQL_VERSION`
- `WIN_POLICY_*`, `TILT_POLICY_*` to tune threshold policy constraints
- `GATE_*` to tune promotion gate thresholds (also used by stage-transition gate checks)

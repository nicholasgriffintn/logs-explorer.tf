# Model registry and versioning policy

Model artefacts are only useful in production if we can trace every prediction back to code, data snapshot, and evaluation.
This policy defines a lightweight V1 registry in Iceberg so we can ship governance now without adding a new platform dependency.

## Registry choice

Use Iceberg-backed registry tables in `tf2.default` for V1:

- `ml_model_registry`
- `ml_model_stage_history`

Create them through the dedicated ML Spark pipeline:

```bash
infra/spark/run_ml_pipeline.sh incremental
```

Baseline training can populate candidate rows directly:

```bash
MODEL_VERSION=v1.0.0 infra/trino/queries/ml/run_ml_baseline_training.sh
```

This command runs training in a dedicated container image so dependency updates are
managed in one place (`infra/ml/Dockerfile`, `infra/ml/requirements.txt`).

Run ML pipeline cadence independently from feature-serving cadence.

## Versioning rules

- Use semantic model versions per task: `vMAJOR.MINOR.PATCH` (example: `v1.2.0`).
- Increment:
  - `MAJOR` for feature set or label definition changes.
  - `MINOR` for algorithm/hyperparameter changes with same inputs/labels.
  - `PATCH` for bug fixes that do not change model semantics.
- Keep one active `production` version per `model_name`.

## Required lineage fields

Every registry row must include:

- `model_name`
- `model_version`
- `snapshot_id` from `ml_training_dataset_snapshots`
- `training_code_version` (git SHA)
- `feature_sql_version` (git SHA)
- `artifact_uri` in object storage
- `metrics_json` and `calibration_notes`

Reject promotions when any required field is missing.

## Stage policy

Allowed stages:

- `candidate`
- `staging`
- `production`
- `archived`

Promotion flow:

- `candidate -> staging -> production`
- write a stage transition row to `ml_model_stage_history` for every change
- set `is_active = true` only for the current production version

Use the stage transition helper for controlled promotions:

```bash
MODEL_NAME=win_probability_baseline \
MODEL_VERSION=v1.0.1 \
TO_STAGE=staging \
CHANGED_BY=ml_engineer \
CHANGE_REASON="passes offline checks" \
infra/trino/queries/ml/run_ml_model_stage_transition.sh
```

Then promote to production:

```bash
MODEL_NAME=win_probability_baseline \
MODEL_VERSION=v1.0.1 \
TO_STAGE=production \
CHANGED_BY=ml_engineer \
CHANGE_REASON="approved for rollout" \
infra/trino/queries/ml/run_ml_model_stage_transition.sh
```

## Rollback policy

- Keep at least one prior production model in `archived` or `staging` with valid artefact access.
- Roll back by promoting the last known good version and recording the incident reason in stage history.
- Never delete registry records; append-only history is required for auditability.

Rollback helper:

```bash
MODEL_NAME=win_probability_baseline \
CHANGED_BY=ml_engineer \
CHANGE_REASON="rollback after quality regression" \
infra/trino/queries/ml/run_ml_model_rollback.sh
```

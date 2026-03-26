# ML training snapshot workflow

Training reproducibility breaks if features move between model runs without a recorded cut-off.
Use this workflow to materialise immutable snapshot datasets from `features_*` tables before any training job starts.

## What this workflow creates

- `tf2.default.ml_training_dataset_snapshots`: snapshot metadata and lineage.
- `tf2.default.ml_training_player_match`: snapshot-scoped training rows with labels.

Snapshot IDs are deterministic per source cut-off (`max(match_time)`), so reruns are idempotent.

## Run the workflow

Run feature-serving refresh through Airflow first:

```bash
infra/airflow/scripts/airflow.sh trigger tf2_feature_serving_daily
```

Snapshots are built by the ML Airflow DAG:

```bash
infra/airflow/scripts/airflow.sh trigger tf2_ml_daily_or_weekly
```

The pipeline step `ml_training_snapshot_refresh` materialises snapshot rows.

## Data included in training rows

- match-level gameplay, contribution, and chat features from `features_player_match`
- score context features (`team_score`, `opponent_score`, `score_delta`) derived from `logs`
- rolling-form context from `features_player_recent_form`
- career form anchors (`career_avg_kills`, `career_avg_damage`, `career_avg_impact`)
- labels:
  - `label_win` from `won_game`
  - `label_impact_percentile` via `NTILE(100)` by `match_date`
  - `label_tilt` from `possible_tilt_label`

## Readiness gate

Run data readiness checks before or alongside snapshots:

```bash
infra/airflow/scripts/airflow.sh trigger tf2_ml_daily_or_weekly
```

## Lineage minimum for model runs

Record these fields with each training run:

- `snapshot_id`
- feature SQL commit SHA
- training code commit SHA
- hyperparameters
- evaluation metrics and calibration notes

Do not train directly from live `features_*` tables in scheduled jobs.

## Baseline training command

Train baseline models from the latest snapshot and upsert candidate rows in `ml_model_registry`:

```bash
infra/airflow/scripts/airflow.sh trigger tf2_ml_daily_or_weekly '{"run_baseline_training": true, "model_version": "v1.0.0"}'
```

The training runner uses a dedicated Docker image and publishes:

- model artefacts in `artifacts/ml/...`
- an offline evaluation report in `docs/ml-offline-evaluation-report.md`
- daily validation metrics in `tf2.default.ml_model_validation_metrics_daily`

Training guards:

- win and impact baselines use pre-match form/context features only
- leakage-prone outcome proxy features are blocked in trainer feature selection
- feature quality controls bucket rare maps and clip numeric outliers using train-only statistics
- win and tilt probabilities are calibrated and scored at an explicit operating threshold policy
- temporal backtesting fold results are published in the offline evaluation report
- promotion gate checks are emitted in the report and model metadata for each candidate

## Stage promotion and rollback

Promote a candidate:

```bash
MODEL_NAME=win_probability_baseline \
MODEL_VERSION=v1.0.0 \
TO_STAGE=staging \
CHANGED_BY=ml_engineer \
CHANGE_REASON="passes offline checks" \
infra/trino/queries/ml/run_ml_model_stage_transition.sh
```

Validate gates manually before promotion (optional, stage transition enforces this by default):

```bash
MODEL_NAME=win_probability_baseline \
MODEL_VERSION=v1.0.0 \
infra/trino/queries/ml/run_ml_promotion_gate_check.sh
```

Rollback to prior version:

```bash
MODEL_NAME=win_probability_baseline \
CHANGED_BY=ml_engineer \
CHANGE_REASON="rollback after regression" \
infra/trino/queries/ml/run_ml_model_rollback.sh
```

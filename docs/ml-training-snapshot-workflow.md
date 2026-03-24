# ML training snapshot workflow

Training reproducibility breaks if features move between model runs without a recorded cut-off.
Use this workflow to materialise immutable snapshot datasets from `features_*` tables before any training job starts.

## What this workflow creates

- `tf2.default.ml_training_dataset_snapshots`: snapshot metadata and lineage.
- `tf2.default.ml_training_player_match`: snapshot-scoped training rows with labels.

Snapshot IDs are deterministic per source cut-off (`max(match_time)`), so reruns are idempotent.

## Run the workflow

Refresh feature tables first:

```bash
infra/spark/run_feature_pipeline.sh incremental
```

Snapshots are built by the dedicated ML Spark pipeline:

```bash
infra/spark/run_ml_pipeline.sh incremental
```

or:

```bash
infra/spark/run_ml_pipeline.sh full
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
infra/trino/queries/run_ml_readiness_check.sh
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
MODEL_VERSION=v1.0.0 infra/trino/queries/run_ml_baseline_training.sh
```

The training runner uses a dedicated Docker image and publishes:

- model artefacts in `artifacts/ml/...`
- an offline evaluation report in `docs/ml-offline-evaluation-report.md`

# ML training snapshot workflow

Training reproducibility breaks if features move between model runs without a recorded cut-off.
Use this workflow to materialise immutable snapshot datasets from `features_*` tables before any training job starts.

## What this workflow creates

- `tf2.default.ml_training_dataset_snapshots`: snapshot metadata and lineage.
- `tf2.default.ml_training_player_match`: snapshot-scoped training rows with labels.

Snapshot IDs are deterministic per source cut-off (`max(match_time)`), so reruns are idempotent.

## Run the workflow

```bash
infra/trino/queries/run_training_snapshot.sh
```

Optional environment override:

- `TRINO_CONTAINER`: non-default Trino container name (default `tf2-trino`)

The runner executes:

- `infra/trino/queries/25_build_ml_training_snapshot.sql`

Then prints the latest snapshot metadata row.

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

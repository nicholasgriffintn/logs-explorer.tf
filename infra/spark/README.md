# Spark processing

Spark owns all processing. Trino is query-serving only.

## Pipelines

### 1) Feature and serving pipeline

Builds:

- `tf2.default.features_player_match`
- `tf2.default.features_player_recent_form`
- `tf2.default.serving_player_profiles`
- `tf2.default.serving_map_overview_daily`
- `tf2.default.serving_player_match_deep_dive`
- `tf2.default.ops_pipeline_runs` entries for feature-serving steps

Run:

```bash
infra/spark/run_feature_pipeline.sh incremental
```

```bash
infra/spark/run_feature_pipeline.sh full
```

### 2) ML pipeline (separate schedule)

Builds/refreshes:

- `tf2.default.ml_training_dataset_snapshots`
- `tf2.default.ml_training_player_match`
- `tf2.default.ml_model_registry`
- `tf2.default.ml_model_stage_history`
- `tf2.default.serving_ml_model_registry`
- `tf2.default.serving_ml_pipeline_progress_daily`
- `tf2.default.ops_pipeline_runs` entries for ML steps

Run:

```bash
infra/spark/run_ml_pipeline.sh incremental
```

```bash
infra/spark/run_ml_pipeline.sh full
```

### 3) Combined pipeline (optional)

Runs feature-serving and ML in one execution.

```bash
infra/spark/run_processing_pipeline.sh incremental all
```

## Config

Create a Spark env file:

```bash
cp infra/spark/spark.env.example infra/spark/spark.env
```

Run with that config:

```bash
SPARK_ENV_FILE=infra/spark/spark.env infra/spark/run_feature_pipeline.sh incremental
```

Required values (in env file or exported):

- `CATALOG_URI`
- `WAREHOUSE`
- `R2_CATALOG_TOKEN`
- `R2_ENDPOINT`
- `R2_ACCESS_KEY_ID`
- `R2_SECRET_ACCESS_KEY`

Optional values:

- `REFRESH_DAYS` (default `7`)
- `SPARK_IMAGE` (default `logs-explorer-spark-processing:latest`)
- `SPARK_NETWORK` (default `logs-explorer`)
- `SPARK_ENV_FILE` (env-file path)
- `SPARK_MASTER` (default `local[4]`)
- `SPARK_DRIVER_MEMORY` (default `6g`)
- `SPARK_EXECUTOR_MEMORY` (default `6g`)
- `SPARK_SQL_SHUFFLE_PARTITIONS` (default `512`)
- `SPARK_DEFAULT_PARALLELISM` (default `256`)

## Recommended cadence

- Feature-serving pipeline: frequent (hourly/daily, depending on ingestion volume).
- ML pipeline: separate and less frequent (for example daily/weekly retraining windows).

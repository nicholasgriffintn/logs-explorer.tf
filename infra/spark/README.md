# Spark processing

Spark owns all processing. Trino is query-serving only.
Spark jobs are submitted by Airflow using `SparkSubmitOperator`.

## Job layout

Spark jobs are grouped by domain under `infra/spark/jobs`:

- `features/`: feature table sources and refresh logic (`features_*`)
- `serving/`: serving table materialisation (`serving_*`)
- `ml/`: ML snapshot and ML serving progress refresh
- `ops/`: pipeline orchestration, run metadata, and shared Spark utilities

Entrypoints:

- `infra/spark/jobs/build_processing.py`
- `infra/spark/jobs/build_features.py`

## Pipelines built by Airflow

Feature-serving pipeline outputs:

- `tf2.default.features_player_match`
- `tf2.default.features_player_recent_form`
- `tf2.default.serving_player_profiles`
- `tf2.default.serving_map_overview_daily`
- `tf2.default.serving_player_match_deep_dive`

ML pipeline outputs:

- `tf2.default.ml_training_dataset_snapshots`
- `tf2.default.ml_training_player_match`
- `tf2.default.ml_model_registry`
- `tf2.default.ml_model_stage_history`
- `tf2.default.ml_model_validation_metrics_daily`
- `tf2.default.serving_ml_model_registry`
- `tf2.default.serving_ml_pipeline_progress_daily`
- `tf2.default.serving_ml_prediction_quality_daily`

Run metadata table:

- `tf2.default.ops_pipeline_runs`

## Configuration contract

Airflow workers read Spark catalog/storage configuration from:

- `infra/spark/spark.env`

Required values:

- `CATALOG_URI`
- `WAREHOUSE`
- `R2_CATALOG_TOKEN`
- `R2_ENDPOINT`
- `R2_ACCESS_KEY_ID`
- `R2_SECRET_ACCESS_KEY`

Optional tuning values:

- `REFRESH_DAYS` (default `7`)
- `SPARK_MASTER` (default `local[4]`)
- `SPARK_DRIVER_MEMORY` (default `6g`)
- `SPARK_EXECUTOR_MEMORY` (default `6g`)
- `SPARK_SQL_SHUFFLE_PARTITIONS` (default `512`)
- `SPARK_DEFAULT_PARALLELISM` (default `256`)
- `SPARK_NLP_VERSION` (default `5.5.3`)
- `SPARK_ICEBERG_VECTORIZATION_ENABLED` (default `false`)
- `SPARK_PARQUET_VECTORIZED_READER_ENABLED` (default `false`)
- `SPARK_PARQUET_NESTED_VECTORIZED_READER_ENABLED` (default `false`)

## Cadence

- Feature-serving DAG: frequent cadence (hourly/daily)
- ML DAG: separate cadence (daily/weekly)

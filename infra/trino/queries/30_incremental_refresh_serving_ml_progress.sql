-- Incrementally refreshes ML serving progress tables.
-- Strategy: full rebuild on each run because source tables are low volume metadata.

CREATE TABLE IF NOT EXISTS tf2.default.ml_training_dataset_snapshots (
  snapshot_id VARCHAR,
  snapshot_cutoff_time TIMESTAMP(6) WITH TIME ZONE,
  snapshot_cutoff_date DATE,
  source_match_rows BIGINT,
  source_recent_form_rows BIGINT,
  training_rows BIGINT,
  created_at TIMESTAMP(6) WITH TIME ZONE
)
WITH (
  format = 'PARQUET',
  partitioning = ARRAY['month(snapshot_cutoff_date)']
);

CREATE TABLE IF NOT EXISTS tf2.default.ml_model_registry (
  model_name VARCHAR,
  model_version VARCHAR,
  task_type VARCHAR,
  stage VARCHAR,
  snapshot_id VARCHAR,
  training_code_version VARCHAR,
  feature_sql_version VARCHAR,
  artifact_uri VARCHAR,
  metrics_json VARCHAR,
  calibration_notes VARCHAR,
  created_at TIMESTAMP(6) WITH TIME ZONE,
  promoted_at TIMESTAMP(6) WITH TIME ZONE,
  is_active BOOLEAN
)
WITH (
  format = 'PARQUET',
  partitioning = ARRAY['model_name']
);

CREATE TABLE IF NOT EXISTS tf2.default.ml_model_stage_history (
  model_name VARCHAR,
  model_version VARCHAR,
  from_stage VARCHAR,
  to_stage VARCHAR,
  changed_by VARCHAR,
  change_reason VARCHAR,
  changed_at TIMESTAMP(6) WITH TIME ZONE
)
WITH (
  format = 'PARQUET',
  partitioning = ARRAY['model_name']
);

CREATE TABLE IF NOT EXISTS tf2.default.ops_pipeline_runs (
  run_id VARCHAR,
  run_mode VARCHAR,
  step_name VARCHAR,
  status VARCHAR,
  started_at TIMESTAMP(3) WITH TIME ZONE,
  finished_at TIMESTAMP(3) WITH TIME ZONE,
  duration_seconds DOUBLE,
  row_count BIGINT,
  error_text VARCHAR,
  created_at TIMESTAMP(3) WITH TIME ZONE
)
WITH (
  format = 'PARQUET',
  partitioning = ARRAY['day(started_at)']
);

CREATE TABLE IF NOT EXISTS tf2.default.serving_ml_model_registry
WITH (
  format = 'PARQUET',
  partitioning = ARRAY['model_name']
) AS
SELECT
  CAST(NULL AS VARCHAR) AS model_name,
  CAST(NULL AS VARCHAR) AS model_version,
  CAST(NULL AS VARCHAR) AS task_type,
  CAST(NULL AS VARCHAR) AS stage,
  CAST(FALSE AS BOOLEAN) AS is_active,
  CAST(NULL AS VARCHAR) AS snapshot_id,
  CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE) AS snapshot_cutoff_time,
  CAST(NULL AS DATE) AS snapshot_cutoff_date,
  CAST(NULL AS BIGINT) AS snapshot_training_rows,
  CAST(NULL AS VARCHAR) AS training_code_version,
  CAST(NULL AS VARCHAR) AS feature_sql_version,
  CAST(NULL AS VARCHAR) AS artifact_uri,
  CAST(NULL AS VARCHAR) AS primary_metric_name,
  CAST(NULL AS DOUBLE) AS primary_metric_value,
  CAST(NULL AS DOUBLE) AS metric_auc,
  CAST(NULL AS DOUBLE) AS metric_roc_auc,
  CAST(NULL AS DOUBLE) AS metric_precision,
  CAST(NULL AS DOUBLE) AS metric_recall,
  CAST(NULL AS DOUBLE) AS metric_f1,
  CAST(NULL AS DOUBLE) AS metric_rmse,
  CAST(NULL AS DOUBLE) AS metric_mae,
  CAST(NULL AS VARCHAR) AS metrics_json,
  CAST(NULL AS VARCHAR) AS calibration_notes,
  CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE) AS created_at,
  CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE) AS promoted_at,
  CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE) AS snapshot_recorded_at,
  CAST(NULL AS BIGINT) AS data_age_days,
  CAST(NULL AS BIGINT) AS model_age_days,
  CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE) AS updated_at
WHERE FALSE;

DELETE FROM tf2.default.serving_ml_model_registry;

INSERT INTO tf2.default.serving_ml_model_registry
WITH snapshot_meta AS (
  SELECT
    snapshot_id,
    MAX_BY(snapshot_cutoff_time, created_at) AS snapshot_cutoff_time,
    MAX_BY(snapshot_cutoff_date, created_at) AS snapshot_cutoff_date,
    MAX_BY(training_rows, created_at) AS snapshot_training_rows,
    MAX(created_at) AS snapshot_recorded_at
  FROM tf2.default.ml_training_dataset_snapshots
  GROUP BY snapshot_id
)
SELECT
  m.model_name,
  m.model_version,
  m.task_type,
  m.stage,
  COALESCE(m.is_active, FALSE) AS is_active,
  m.snapshot_id,
  sm.snapshot_cutoff_time,
  sm.snapshot_cutoff_date,
  sm.snapshot_training_rows,
  m.training_code_version,
  m.feature_sql_version,
  m.artifact_uri,
  CASE
    WHEN TRY(CAST(json_extract_scalar(m.metrics_json, '$.auc') AS DOUBLE)) IS NOT NULL THEN 'auc'
    WHEN TRY(CAST(json_extract_scalar(m.metrics_json, '$.roc_auc') AS DOUBLE)) IS NOT NULL THEN 'roc_auc'
    WHEN TRY(CAST(json_extract_scalar(m.metrics_json, '$.f1') AS DOUBLE)) IS NOT NULL THEN 'f1'
    WHEN TRY(CAST(json_extract_scalar(m.metrics_json, '$.precision') AS DOUBLE)) IS NOT NULL THEN 'precision'
    WHEN TRY(CAST(json_extract_scalar(m.metrics_json, '$.recall') AS DOUBLE)) IS NOT NULL THEN 'recall'
    WHEN TRY(CAST(json_extract_scalar(m.metrics_json, '$.rmse') AS DOUBLE)) IS NOT NULL THEN 'rmse'
    WHEN TRY(CAST(json_extract_scalar(m.metrics_json, '$.mae') AS DOUBLE)) IS NOT NULL THEN 'mae'
    ELSE NULL
  END AS primary_metric_name,
  COALESCE(
    TRY(CAST(json_extract_scalar(m.metrics_json, '$.auc') AS DOUBLE)),
    TRY(CAST(json_extract_scalar(m.metrics_json, '$.roc_auc') AS DOUBLE)),
    TRY(CAST(json_extract_scalar(m.metrics_json, '$.f1') AS DOUBLE)),
    TRY(CAST(json_extract_scalar(m.metrics_json, '$.precision') AS DOUBLE)),
    TRY(CAST(json_extract_scalar(m.metrics_json, '$.recall') AS DOUBLE)),
    TRY(CAST(json_extract_scalar(m.metrics_json, '$.rmse') AS DOUBLE)),
    TRY(CAST(json_extract_scalar(m.metrics_json, '$.mae') AS DOUBLE))
  ) AS primary_metric_value,
  TRY(CAST(json_extract_scalar(m.metrics_json, '$.auc') AS DOUBLE)) AS metric_auc,
  TRY(CAST(json_extract_scalar(m.metrics_json, '$.roc_auc') AS DOUBLE)) AS metric_roc_auc,
  TRY(CAST(json_extract_scalar(m.metrics_json, '$.precision') AS DOUBLE)) AS metric_precision,
  TRY(CAST(json_extract_scalar(m.metrics_json, '$.recall') AS DOUBLE)) AS metric_recall,
  TRY(CAST(json_extract_scalar(m.metrics_json, '$.f1') AS DOUBLE)) AS metric_f1,
  TRY(CAST(json_extract_scalar(m.metrics_json, '$.rmse') AS DOUBLE)) AS metric_rmse,
  TRY(CAST(json_extract_scalar(m.metrics_json, '$.mae') AS DOUBLE)) AS metric_mae,
  m.metrics_json,
  m.calibration_notes,
  m.created_at,
  m.promoted_at,
  sm.snapshot_recorded_at,
  DATE_DIFF('day', CAST(COALESCE(sm.snapshot_cutoff_time, m.created_at) AS DATE), CURRENT_DATE) AS data_age_days,
  DATE_DIFF('day', CAST(COALESCE(m.promoted_at, m.created_at) AS DATE), CURRENT_DATE) AS model_age_days,
  CURRENT_TIMESTAMP AS updated_at
FROM tf2.default.ml_model_registry m
LEFT JOIN snapshot_meta sm
  ON sm.snapshot_id = m.snapshot_id;

CREATE TABLE IF NOT EXISTS tf2.default.serving_ml_pipeline_progress_daily
WITH (
  format = 'PARQUET',
  partitioning = ARRAY['month(progress_date)']
) AS
SELECT
  CAST(NULL AS DATE) AS progress_date,
  CAST(NULL AS BIGINT) AS pipeline_success_runs,
  CAST(NULL AS BIGINT) AS pipeline_failed_runs,
  CAST(NULL AS DOUBLE) AS avg_pipeline_duration_seconds,
  CAST(NULL AS VARCHAR) AS latest_pipeline_status,
  CAST(NULL AS BIGINT) AS snapshots_created,
  CAST(NULL AS BIGINT) AS training_rows_materialised,
  CAST(NULL AS VARCHAR) AS latest_snapshot_id,
  CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE) AS latest_snapshot_created_at,
  CAST(NULL AS BIGINT) AS models_registered,
  CAST(NULL AS BIGINT) AS candidate_models_registered,
  CAST(NULL AS BIGINT) AS staging_models_registered,
  CAST(NULL AS BIGINT) AS production_models_registered,
  CAST(NULL AS BIGINT) AS promotions_to_staging,
  CAST(NULL AS BIGINT) AS promotions_to_production,
  CAST(NULL AS BIGINT) AS active_production_models,
  CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE) AS updated_at
WHERE FALSE;

DELETE FROM tf2.default.serving_ml_pipeline_progress_daily;

INSERT INTO tf2.default.serving_ml_pipeline_progress_daily
WITH pipeline_daily AS (
  SELECT
    CAST(started_at AS DATE) AS progress_date,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS pipeline_success_runs,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS pipeline_failed_runs,
    ROUND(AVG(duration_seconds), 3) AS avg_pipeline_duration_seconds,
    MAX_BY(status, started_at) AS latest_pipeline_status
  FROM tf2.default.ops_pipeline_runs
  WHERE step_name = 'pipeline'
  GROUP BY CAST(started_at AS DATE)
),
snapshot_daily AS (
  SELECT
    snapshot_cutoff_date AS progress_date,
    COUNT(*) AS snapshots_created,
    SUM(training_rows) AS training_rows_materialised,
    MAX(created_at) AS latest_snapshot_created_at,
    MAX_BY(snapshot_id, created_at) AS latest_snapshot_id
  FROM tf2.default.ml_training_dataset_snapshots
  GROUP BY snapshot_cutoff_date
),
registry_daily AS (
  SELECT
    CAST(created_at AS DATE) AS progress_date,
    COUNT(*) AS models_registered,
    SUM(CASE WHEN stage = 'candidate' THEN 1 ELSE 0 END) AS candidate_models_registered,
    SUM(CASE WHEN stage = 'staging' THEN 1 ELSE 0 END) AS staging_models_registered,
    SUM(CASE WHEN stage = 'production' THEN 1 ELSE 0 END) AS production_models_registered
  FROM tf2.default.ml_model_registry
  GROUP BY CAST(created_at AS DATE)
),
promotion_daily AS (
  SELECT
    CAST(changed_at AS DATE) AS progress_date,
    SUM(CASE WHEN to_stage = 'staging' THEN 1 ELSE 0 END) AS promotions_to_staging,
    SUM(CASE WHEN to_stage = 'production' THEN 1 ELSE 0 END) AS promotions_to_production
  FROM tf2.default.ml_model_stage_history
  GROUP BY CAST(changed_at AS DATE)
),
active_models AS (
  SELECT
    SUM(CASE WHEN stage = 'production' AND COALESCE(is_active, FALSE) THEN 1 ELSE 0 END)
      AS active_production_models
  FROM tf2.default.ml_model_registry
),
progress_dates AS (
  SELECT progress_date FROM pipeline_daily
  UNION
  SELECT progress_date FROM snapshot_daily
  UNION
  SELECT progress_date FROM registry_daily
  UNION
  SELECT progress_date FROM promotion_daily
)
SELECT
  d.progress_date,
  COALESCE(pd.pipeline_success_runs, 0) AS pipeline_success_runs,
  COALESCE(pd.pipeline_failed_runs, 0) AS pipeline_failed_runs,
  COALESCE(pd.avg_pipeline_duration_seconds, 0.0) AS avg_pipeline_duration_seconds,
  COALESCE(pd.latest_pipeline_status, 'unknown') AS latest_pipeline_status,
  COALESCE(sd.snapshots_created, 0) AS snapshots_created,
  COALESCE(sd.training_rows_materialised, 0) AS training_rows_materialised,
  sd.latest_snapshot_id,
  sd.latest_snapshot_created_at,
  COALESCE(rd.models_registered, 0) AS models_registered,
  COALESCE(rd.candidate_models_registered, 0) AS candidate_models_registered,
  COALESCE(rd.staging_models_registered, 0) AS staging_models_registered,
  COALESCE(rd.production_models_registered, 0) AS production_models_registered,
  COALESCE(prd.promotions_to_staging, 0) AS promotions_to_staging,
  COALESCE(prd.promotions_to_production, 0) AS promotions_to_production,
  COALESCE(am.active_production_models, 0) AS active_production_models,
  CURRENT_TIMESTAMP AS updated_at
FROM progress_dates d
LEFT JOIN pipeline_daily pd
  ON pd.progress_date = d.progress_date
LEFT JOIN snapshot_daily sd
  ON sd.progress_date = d.progress_date
LEFT JOIN registry_daily rd
  ON rd.progress_date = d.progress_date
LEFT JOIN promotion_daily prd
  ON prd.progress_date = d.progress_date
CROSS JOIN active_models am;

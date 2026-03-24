-- Dashboard pack: ML progress and model registry status.
-- Contract dependencies: serving_ml_pipeline_progress_daily + serving_ml_model_registry.

-- Section 1: daily ML and pipeline progress.
WITH params AS (
  SELECT DATE_ADD('day', -90, CURRENT_DATE) AS start_date
)
SELECT
  progress_date,
  pipeline_success_runs,
  pipeline_failed_runs,
  avg_pipeline_duration_seconds,
  latest_pipeline_status,
  snapshots_created,
  training_rows_materialised,
  latest_snapshot_id,
  models_registered,
  candidate_models_registered,
  staging_models_registered,
  production_models_registered,
  promotions_to_staging,
  promotions_to_production,
  active_production_models,
  updated_at
FROM tf2.default.serving_ml_pipeline_progress_daily smppd
CROSS JOIN params p
WHERE smppd.progress_date >= p.start_date
ORDER BY progress_date DESC;

-- Section 2: model-version registry snapshot.
SELECT
  model_name,
  model_version,
  task_type,
  stage,
  is_active,
  snapshot_id,
  snapshot_cutoff_date,
  snapshot_training_rows,
  primary_metric_name,
  ROUND(primary_metric_value, 6) AS primary_metric_value,
  metric_auc,
  metric_precision,
  metric_recall,
  metric_f1,
  metric_rmse,
  metric_mae,
  model_age_days,
  data_age_days,
  created_at,
  promoted_at,
  updated_at
FROM tf2.default.serving_ml_model_registry
ORDER BY COALESCE(promoted_at, created_at) DESC, model_name, model_version;

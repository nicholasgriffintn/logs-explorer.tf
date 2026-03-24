-- Dashboard pack: ML prediction quality over time.
-- Contract dependency: tf2.default.serving_ml_prediction_quality_daily.

-- Section 1: recent quality trend by model version.
WITH params AS (
  SELECT DATE_ADD('day', -90, CURRENT_DATE) AS start_date
)
SELECT
  progress_date,
  model_name,
  model_version,
  task_type,
  stage,
  is_active,
  rows_total,
  observed_positive_rate,
  predicted_positive_rate,
  precision,
  recall,
  f1,
  roc_auc,
  pr_auc,
  brier,
  rmse,
  mae,
  data_age_days,
  updated_at
FROM tf2.default.serving_ml_prediction_quality_daily smpqd
CROSS JOIN params p
WHERE smpqd.progress_date >= p.start_date
ORDER BY progress_date DESC, model_name, model_version;

-- Section 2: latest available quality snapshot by model version.
WITH latest_per_model AS (
  SELECT
    model_name,
    model_version,
    MAX(progress_date) AS latest_progress_date
  FROM tf2.default.serving_ml_prediction_quality_daily
  GROUP BY model_name, model_version
)
SELECT
  q.model_name,
  q.model_version,
  q.task_type,
  q.stage,
  q.is_active,
  q.snapshot_id,
  q.progress_date,
  q.rows_total,
  q.precision,
  q.recall,
  q.f1,
  q.roc_auc,
  q.pr_auc,
  q.brier,
  q.rmse,
  q.mae,
  q.data_age_days,
  q.updated_at
FROM tf2.default.serving_ml_prediction_quality_daily q
JOIN latest_per_model lpm
  ON lpm.model_name = q.model_name
 AND lpm.model_version = q.model_version
 AND lpm.latest_progress_date = q.progress_date
ORDER BY q.model_name, q.model_version;

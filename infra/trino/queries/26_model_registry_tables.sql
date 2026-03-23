-- Creates lightweight model registry tables in Iceberg for V1.
-- Use these tables to track lineage, metrics, and promotion history.

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

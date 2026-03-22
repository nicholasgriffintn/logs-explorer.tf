-- Creates the run metadata table used by pipeline runners.

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

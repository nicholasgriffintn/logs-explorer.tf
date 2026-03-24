from __future__ import annotations

from pyspark.sql import SparkSession


def refresh_ml_progress_serving_tables(spark: SparkSession) -> None:
    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW snapshot_meta AS
        SELECT
          snapshot_id,
          snapshot_cutoff_time,
          snapshot_cutoff_date,
          training_rows AS snapshot_training_rows,
          created_at AS snapshot_recorded_at
        FROM (
          SELECT
            s.*,
            ROW_NUMBER() OVER (PARTITION BY snapshot_id ORDER BY created_at DESC) AS rn
          FROM tf2.default.ml_training_dataset_snapshots s
        ) ranked
        WHERE rn = 1
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TABLE tf2.default.serving_ml_model_registry
        USING iceberg
        PARTITIONED BY (model_name)
        AS
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
            WHEN CAST(get_json_object(m.metrics_json, '$.auc') AS DOUBLE) IS NOT NULL THEN 'auc'
            WHEN CAST(get_json_object(m.metrics_json, '$.roc_auc') AS DOUBLE) IS NOT NULL THEN 'roc_auc'
            WHEN CAST(get_json_object(m.metrics_json, '$.f1') AS DOUBLE) IS NOT NULL THEN 'f1'
            WHEN CAST(get_json_object(m.metrics_json, '$.precision') AS DOUBLE) IS NOT NULL THEN 'precision'
            WHEN CAST(get_json_object(m.metrics_json, '$.recall') AS DOUBLE) IS NOT NULL THEN 'recall'
            WHEN CAST(get_json_object(m.metrics_json, '$.rmse') AS DOUBLE) IS NOT NULL THEN 'rmse'
            WHEN CAST(get_json_object(m.metrics_json, '$.mae') AS DOUBLE) IS NOT NULL THEN 'mae'
            ELSE NULL
          END AS primary_metric_name,
          COALESCE(
            CAST(get_json_object(m.metrics_json, '$.auc') AS DOUBLE),
            CAST(get_json_object(m.metrics_json, '$.roc_auc') AS DOUBLE),
            CAST(get_json_object(m.metrics_json, '$.f1') AS DOUBLE),
            CAST(get_json_object(m.metrics_json, '$.precision') AS DOUBLE),
            CAST(get_json_object(m.metrics_json, '$.recall') AS DOUBLE),
            CAST(get_json_object(m.metrics_json, '$.rmse') AS DOUBLE),
            CAST(get_json_object(m.metrics_json, '$.mae') AS DOUBLE)
          ) AS primary_metric_value,
          CAST(get_json_object(m.metrics_json, '$.auc') AS DOUBLE) AS metric_auc,
          CAST(get_json_object(m.metrics_json, '$.roc_auc') AS DOUBLE) AS metric_roc_auc,
          CAST(get_json_object(m.metrics_json, '$.precision') AS DOUBLE) AS metric_precision,
          CAST(get_json_object(m.metrics_json, '$.recall') AS DOUBLE) AS metric_recall,
          CAST(get_json_object(m.metrics_json, '$.f1') AS DOUBLE) AS metric_f1,
          CAST(get_json_object(m.metrics_json, '$.rmse') AS DOUBLE) AS metric_rmse,
          CAST(get_json_object(m.metrics_json, '$.mae') AS DOUBLE) AS metric_mae,
          m.metrics_json,
          m.calibration_notes,
          m.created_at,
          m.promoted_at,
          sm.snapshot_recorded_at,
          DATEDIFF(
            CURRENT_DATE(),
            CAST(COALESCE(sm.snapshot_cutoff_time, m.created_at) AS DATE)
          ) AS data_age_days,
          DATEDIFF(
            CURRENT_DATE(),
            CAST(COALESCE(m.promoted_at, m.created_at) AS DATE)
          ) AS model_age_days,
          CURRENT_TIMESTAMP() AS updated_at
        FROM tf2.default.ml_model_registry m
        LEFT JOIN snapshot_meta sm
          ON sm.snapshot_id = m.snapshot_id
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW pipeline_daily AS
        SELECT
          CAST(started_at AS DATE) AS progress_date,
          SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS pipeline_success_runs,
          SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS pipeline_failed_runs,
          ROUND(AVG(duration_seconds), 3) AS avg_pipeline_duration_seconds
        FROM tf2.default.ops_pipeline_runs
        WHERE step_name = 'pipeline'
           OR step_name LIKE 'pipeline_%'
        GROUP BY CAST(started_at AS DATE)
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW pipeline_latest AS
        SELECT
          progress_date,
          status AS latest_pipeline_status
        FROM (
          SELECT
            CAST(started_at AS DATE) AS progress_date,
            status,
            ROW_NUMBER() OVER (
              PARTITION BY CAST(started_at AS DATE)
              ORDER BY started_at DESC
            ) AS rn
          FROM tf2.default.ops_pipeline_runs
          WHERE step_name = 'pipeline'
             OR step_name LIKE 'pipeline_%'
        ) ranked
        WHERE rn = 1
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW snapshot_daily AS
        SELECT
          snapshot_cutoff_date AS progress_date,
          COUNT(*) AS snapshots_created,
          SUM(training_rows) AS training_rows_materialised,
          MAX(created_at) AS latest_snapshot_created_at
        FROM tf2.default.ml_training_dataset_snapshots
        GROUP BY snapshot_cutoff_date
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW snapshot_latest AS
        SELECT
          progress_date,
          snapshot_id AS latest_snapshot_id
        FROM (
          SELECT
            snapshot_cutoff_date AS progress_date,
            snapshot_id,
            created_at,
            ROW_NUMBER() OVER (
              PARTITION BY snapshot_cutoff_date
              ORDER BY created_at DESC
            ) AS rn
          FROM tf2.default.ml_training_dataset_snapshots
        ) ranked
        WHERE rn = 1
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW registry_daily AS
        SELECT
          CAST(created_at AS DATE) AS progress_date,
          COUNT(*) AS models_registered,
          SUM(CASE WHEN stage = 'candidate' THEN 1 ELSE 0 END) AS candidate_models_registered,
          SUM(CASE WHEN stage = 'staging' THEN 1 ELSE 0 END) AS staging_models_registered,
          SUM(CASE WHEN stage = 'production' THEN 1 ELSE 0 END) AS production_models_registered
        FROM tf2.default.ml_model_registry
        GROUP BY CAST(created_at AS DATE)
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW promotion_daily AS
        SELECT
          CAST(changed_at AS DATE) AS progress_date,
          SUM(CASE WHEN to_stage = 'staging' THEN 1 ELSE 0 END) AS promotions_to_staging,
          SUM(CASE WHEN to_stage = 'production' THEN 1 ELSE 0 END) AS promotions_to_production
        FROM tf2.default.ml_model_stage_history
        GROUP BY CAST(changed_at AS DATE)
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW active_models AS
        SELECT
          SUM(CASE WHEN stage = 'production' AND COALESCE(is_active, FALSE) THEN 1 ELSE 0 END)
            AS active_production_models
        FROM tf2.default.ml_model_registry
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW progress_dates AS
        SELECT progress_date FROM pipeline_daily
        UNION
        SELECT progress_date FROM snapshot_daily
        UNION
        SELECT progress_date FROM registry_daily
        UNION
        SELECT progress_date FROM promotion_daily
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TABLE tf2.default.serving_ml_pipeline_progress_daily
        USING iceberg
        PARTITIONED BY (months(progress_date))
        AS
        SELECT
          d.progress_date,
          COALESCE(pd.pipeline_success_runs, 0) AS pipeline_success_runs,
          COALESCE(pd.pipeline_failed_runs, 0) AS pipeline_failed_runs,
          COALESCE(pd.avg_pipeline_duration_seconds, 0.0) AS avg_pipeline_duration_seconds,
          COALESCE(pl.latest_pipeline_status, 'unknown') AS latest_pipeline_status,
          COALESCE(sd.snapshots_created, 0) AS snapshots_created,
          COALESCE(sd.training_rows_materialised, 0) AS training_rows_materialised,
          sl.latest_snapshot_id,
          sd.latest_snapshot_created_at,
          COALESCE(rd.models_registered, 0) AS models_registered,
          COALESCE(rd.candidate_models_registered, 0) AS candidate_models_registered,
          COALESCE(rd.staging_models_registered, 0) AS staging_models_registered,
          COALESCE(rd.production_models_registered, 0) AS production_models_registered,
          COALESCE(prd.promotions_to_staging, 0) AS promotions_to_staging,
          COALESCE(prd.promotions_to_production, 0) AS promotions_to_production,
          COALESCE(am.active_production_models, 0) AS active_production_models,
          CURRENT_TIMESTAMP() AS updated_at
        FROM progress_dates d
        LEFT JOIN pipeline_daily pd
          ON pd.progress_date = d.progress_date
        LEFT JOIN pipeline_latest pl
          ON pl.progress_date = d.progress_date
        LEFT JOIN snapshot_daily sd
          ON sd.progress_date = d.progress_date
        LEFT JOIN snapshot_latest sl
          ON sl.progress_date = d.progress_date
        LEFT JOIN registry_daily rd
          ON rd.progress_date = d.progress_date
        LEFT JOIN promotion_daily prd
          ON prd.progress_date = d.progress_date
        CROSS JOIN active_models am
        """
    )

from __future__ import annotations

from pyspark.sql import SparkSession


def ensure_core_pipeline_tables(spark: SparkSession) -> None:
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS tf2.default.ops_pipeline_runs (
          run_id STRING,
          run_mode STRING,
          step_name STRING,
          status STRING,
          started_at TIMESTAMP,
          finished_at TIMESTAMP,
          duration_seconds DOUBLE,
          row_count BIGINT,
          error_text STRING,
          created_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(started_at))
        """
    )


def ensure_ml_tables(spark: SparkSession) -> None:
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS tf2.default.ml_model_registry (
          model_name STRING,
          model_version STRING,
          task_type STRING,
          stage STRING,
          snapshot_id STRING,
          training_code_version STRING,
          feature_sql_version STRING,
          artifact_uri STRING,
          metrics_json STRING,
          calibration_notes STRING,
          created_at TIMESTAMP,
          promoted_at TIMESTAMP,
          is_active BOOLEAN
        )
        USING iceberg
        PARTITIONED BY (model_name)
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS tf2.default.ml_model_stage_history (
          model_name STRING,
          model_version STRING,
          from_stage STRING,
          to_stage STRING,
          changed_by STRING,
          change_reason STRING,
          changed_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (model_name)
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS tf2.default.ml_training_dataset_snapshots (
          snapshot_id STRING,
          snapshot_cutoff_time TIMESTAMP,
          snapshot_cutoff_date DATE,
          source_match_rows BIGINT,
          source_recent_form_rows BIGINT,
          training_rows BIGINT,
          created_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (months(snapshot_cutoff_date))
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS tf2.default.ml_training_player_match (
          snapshot_id STRING,
          snapshot_cutoff_time TIMESTAMP,
          snapshot_cutoff_date DATE,
          steamid STRING,
          logid BIGINT,
          match_time TIMESTAMP,
          match_date DATE,
          map STRING,
          team STRING,
          team_score BIGINT,
          opponent_score BIGINT,
          score_delta BIGINT,
          duration_seconds BIGINT,
          kills BIGINT,
          assists BIGINT,
          deaths BIGINT,
          damage_dealt BIGINT,
          healing_done BIGINT,
          ubers_used BIGINT,
          classes_played_count BIGINT,
          kill_share_of_team DOUBLE,
          damage_share_of_team DOUBLE,
          healing_share_of_team DOUBLE,
          impact_index DOUBLE,
          damage_per_minute DOUBLE,
          kda_ratio DOUBLE,
          chat_messages BIGINT,
          avg_message_length DOUBLE,
          all_caps_messages BIGINT,
          intense_punctuation_messages BIGINT,
          negative_lexicon_hits BIGINT,
          negative_chat_ratio DOUBLE,
          rolling_5_avg_kills DOUBLE,
          rolling_10_avg_damage DOUBLE,
          rolling_10_avg_impact DOUBLE,
          rolling_10_kda_ratio DOUBLE,
          rolling_10_win_rate DOUBLE,
          rolling_10_negative_chat_ratio DOUBLE,
          career_avg_kills DOUBLE,
          career_avg_damage DOUBLE,
          career_avg_impact DOUBLE,
          form_delta_kills DOUBLE,
          form_delta_damage DOUBLE,
          form_delta_impact DOUBLE,
          momentum_label STRING,
          games_played_to_date BIGINT,
          label_win INT,
          label_impact_percentile INT,
          label_tilt INT,
          created_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (snapshot_id)
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS tf2.default.ml_model_validation_metrics_daily (
          model_name STRING,
          model_version STRING,
          task_type STRING,
          snapshot_id STRING,
          progress_date DATE,
          rows_total BIGINT,
          observed_positive_rate DOUBLE,
          predicted_positive_rate DOUBLE,
          precision DOUBLE,
          recall DOUBLE,
          f1 DOUBLE,
          roc_auc DOUBLE,
          pr_auc DOUBLE,
          brier DOUBLE,
          rmse DOUBLE,
          mae DOUBLE,
          created_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (model_name, months(progress_date))
        """
    )

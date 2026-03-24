from __future__ import annotations

from pyspark.sql import SparkSession


def _snapshot_candidate(spark: SparkSession) -> tuple[str | None, str | None]:
    row = spark.sql(
        """
        SELECT
          CONCAT('train_', CAST(CAST(UNIX_TIMESTAMP(MAX(match_time)) AS BIGINT) AS STRING)) AS snapshot_id,
          CAST(MAX(match_time) AS TIMESTAMP) AS snapshot_cutoff_time
        FROM tf2.default.features_player_match
        """
    ).collect()[0]

    return row["snapshot_id"], row["snapshot_cutoff_time"]


def build_training_snapshot(spark: SparkSession) -> None:
    snapshot_id, snapshot_cutoff_time = _snapshot_candidate(spark)
    if snapshot_id is None or snapshot_cutoff_time is None:
        return

    spark.sql(
        f"""
        INSERT INTO tf2.default.ml_training_dataset_snapshots
        SELECT
          '{snapshot_id}' AS snapshot_id,
          CAST('{snapshot_cutoff_time}' AS TIMESTAMP) AS snapshot_cutoff_time,
          CAST('{snapshot_cutoff_time}' AS DATE) AS snapshot_cutoff_date,
          CAST((SELECT COUNT(*) FROM tf2.default.features_player_match) AS BIGINT) AS source_match_rows,
          CAST((SELECT COUNT(*) FROM tf2.default.features_player_recent_form) AS BIGINT) AS source_recent_form_rows,
          CAST((SELECT COUNT(*) FROM tf2.default.features_player_match) AS BIGINT) AS training_rows,
          CURRENT_TIMESTAMP() AS created_at
        WHERE NOT EXISTS (
          SELECT 1
          FROM tf2.default.ml_training_dataset_snapshots s
          WHERE s.snapshot_id = '{snapshot_id}'
        )
        """
    )

    already_materialised = spark.sql(
        f"""
        SELECT COUNT(*) AS rows_existing
        FROM tf2.default.ml_training_player_match
        WHERE snapshot_id = '{snapshot_id}'
        """
    ).collect()[0]["rows_existing"]

    if already_materialised > 0:
        return

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW logs_by_record_for_snapshot AS
        SELECT * FROM (
          SELECT
            l.*,
            ROW_NUMBER() OVER (PARTITION BY l.recordid ORDER BY l.__ingest_ts DESC) AS rn
          FROM tf2.default.logs l
        ) ranked
        WHERE rn = 1
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW logs_base_for_snapshot AS
        SELECT * FROM (
          SELECT
            l.*,
            ROW_NUMBER() OVER (
              PARTITION BY l.logid
              ORDER BY l.__ingest_ts DESC, l.sourcedateepochseconds DESC
            ) AS rn_log
          FROM logs_by_record_for_snapshot l
        ) ranked
        WHERE rn_log = 1
        """
    )

    spark.sql(
        f"""
        INSERT INTO tf2.default.ml_training_player_match
        SELECT
          '{snapshot_id}' AS snapshot_id,
          CAST('{snapshot_cutoff_time}' AS TIMESTAMP) AS snapshot_cutoff_time,
          CAST('{snapshot_cutoff_time}' AS DATE) AS snapshot_cutoff_date,
          fpm.steamid,
          CAST(fpm.logid AS BIGINT) AS logid,
          CAST(fpm.match_time AS TIMESTAMP) AS match_time,
          fpm.match_date,
          fpm.map,
          fpm.team,
          CAST(
            CASE
              WHEN fpm.team = 'Red' THEN COALESCE(lb.redscore, 0)
              WHEN fpm.team = 'Blue' THEN COALESCE(lb.bluescore, 0)
              ELSE 0
            END AS BIGINT
          ) AS team_score,
          CAST(
            CASE
              WHEN fpm.team = 'Red' THEN COALESCE(lb.bluescore, 0)
              WHEN fpm.team = 'Blue' THEN COALESCE(lb.redscore, 0)
              ELSE 0
            END AS BIGINT
          ) AS opponent_score,
          CAST(
            CASE
              WHEN fpm.team = 'Red' THEN COALESCE(lb.redscore, 0) - COALESCE(lb.bluescore, 0)
              WHEN fpm.team = 'Blue' THEN COALESCE(lb.bluescore, 0) - COALESCE(lb.redscore, 0)
              ELSE 0
            END AS BIGINT
          ) AS score_delta,
          CAST(fpm.duration_seconds AS BIGINT) AS duration_seconds,
          CAST(fpm.kills AS BIGINT) AS kills,
          CAST(fpm.assists AS BIGINT) AS assists,
          CAST(fpm.deaths AS BIGINT) AS deaths,
          CAST(fpm.damage_dealt AS BIGINT) AS damage_dealt,
          CAST(fpm.healing_done AS BIGINT) AS healing_done,
          CAST(fpm.ubers_used AS BIGINT) AS ubers_used,
          CAST(fpm.classes_played_count AS BIGINT) AS classes_played_count,
          CAST(fpm.kill_share_of_team AS DOUBLE) AS kill_share_of_team,
          CAST(fpm.damage_share_of_team AS DOUBLE) AS damage_share_of_team,
          CAST(fpm.healing_share_of_team AS DOUBLE) AS healing_share_of_team,
          CAST(fpm.impact_index AS DOUBLE) AS impact_index,
          CAST(fpm.damage_per_minute AS DOUBLE) AS damage_per_minute,
          CAST(fpm.kda_ratio AS DOUBLE) AS kda_ratio,
          CAST(fpm.chat_messages AS BIGINT) AS chat_messages,
          CAST(fpm.avg_message_length AS DOUBLE) AS avg_message_length,
          CAST(fpm.all_caps_messages AS BIGINT) AS all_caps_messages,
          CAST(fpm.intense_punctuation_messages AS BIGINT) AS intense_punctuation_messages,
          CAST(fpm.negative_lexicon_hits AS BIGINT) AS negative_lexicon_hits,
          CAST(fpm.negative_chat_ratio AS DOUBLE) AS negative_chat_ratio,
          CAST(frf.rolling_5_avg_kills AS DOUBLE) AS rolling_5_avg_kills,
          CAST(frf.rolling_10_avg_damage AS DOUBLE) AS rolling_10_avg_damage,
          CAST(frf.rolling_10_avg_impact AS DOUBLE) AS rolling_10_avg_impact,
          CAST(frf.rolling_10_kda_ratio AS DOUBLE) AS rolling_10_kda_ratio,
          CAST(frf.rolling_10_win_rate AS DOUBLE) AS rolling_10_win_rate,
          CAST(frf.rolling_10_negative_chat_ratio AS DOUBLE) AS rolling_10_negative_chat_ratio,
          CAST(frf.career_avg_kills AS DOUBLE) AS career_avg_kills,
          CAST(frf.career_avg_damage AS DOUBLE) AS career_avg_damage,
          CAST(frf.career_avg_impact AS DOUBLE) AS career_avg_impact,
          CAST(frf.form_delta_kills AS DOUBLE) AS form_delta_kills,
          CAST(frf.form_delta_damage AS DOUBLE) AS form_delta_damage,
          CAST(frf.form_delta_impact AS DOUBLE) AS form_delta_impact,
          frf.momentum_label,
          CAST(frf.games_played_to_date AS BIGINT) AS games_played_to_date,
          CAST(fpm.won_game AS INT) AS label_win,
          NTILE(100) OVER (
            PARTITION BY fpm.match_date
            ORDER BY fpm.impact_index, fpm.logid, fpm.steamid
          ) AS label_impact_percentile,
          CAST(fpm.possible_tilt_label AS INT) AS label_tilt,
          CURRENT_TIMESTAMP() AS created_at
        FROM tf2.default.features_player_match fpm
        LEFT JOIN logs_base_for_snapshot lb
          ON lb.logid = fpm.logid
        LEFT JOIN tf2.default.features_player_recent_form frf
          ON frf.steamid = fpm.steamid
         AND frf.logid = fpm.logid
        WHERE fpm.match_time <= CAST('{snapshot_cutoff_time}' AS TIMESTAMP)
        """
    )

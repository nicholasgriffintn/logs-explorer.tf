from __future__ import annotations

from pyspark.sql import SparkSession

from spark_utils import table_exists


SERVING_PLAYER_PROFILES_TABLE = "tf2.default.serving_player_profiles"
SERVING_MAP_OVERVIEW_TABLE = "tf2.default.serving_map_overview_daily"
SERVING_DEEP_DIVE_TABLE = "tf2.default.serving_player_match_deep_dive"


def _create_changed_players_view(spark: SparkSession, mode: str, refresh_days: int) -> None:
    if mode == "full":
        spark.sql(
            """
            CREATE OR REPLACE TEMP VIEW changed_players AS
            SELECT DISTINCT steamid
            FROM tf2.default.features_player_match
            """
        )
        return

    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW serving_bounds AS
        SELECT COALESCE(
          DATE_SUB(MAX(match_date), {refresh_days}),
          CAST('1970-01-01' AS DATE)
        ) AS refresh_start_date
        FROM tf2.default.features_player_match
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW changed_players AS
        SELECT DISTINCT fpm.steamid
        FROM tf2.default.features_player_match fpm
        CROSS JOIN serving_bounds b
        WHERE fpm.match_date >= b.refresh_start_date
        """
    )


def refresh_serving_player_profiles(spark: SparkSession, mode: str, refresh_days: int) -> None:
    _create_changed_players_view(spark, mode, refresh_days)

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW latest_form AS
        SELECT
          steamid,
          match_time AS latest_match_time,
          rolling_5_avg_kills,
          rolling_10_avg_damage,
          rolling_10_avg_impact,
          rolling_10_kda_ratio,
          rolling_10_win_rate,
          rolling_10_negative_chat_ratio,
          form_delta_kills,
          form_delta_damage,
          form_delta_impact,
          momentum_label,
          ROW_NUMBER() OVER (PARTITION BY steamid ORDER BY match_time DESC) AS rn
        FROM tf2.default.features_player_recent_form
        WHERE steamid IN (SELECT steamid FROM changed_players)
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW recent_30 AS
        SELECT
          steamid,
          AVG(kills) AS recent_30_avg_kills,
          AVG(damage_dealt) AS recent_30_avg_damage,
          AVG(impact_index) AS recent_30_avg_impact,
          AVG(CAST(won_game AS DOUBLE)) AS recent_30_win_rate
        FROM (
          SELECT
            steamid,
            kills,
            damage_dealt,
            impact_index,
            won_game,
            ROW_NUMBER() OVER (PARTITION BY steamid ORDER BY match_time DESC) AS rn
          FROM tf2.default.features_player_match
          WHERE steamid IN (SELECT steamid FROM changed_players)
        ) recent
        WHERE rn <= 30
        GROUP BY steamid
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW serving_player_profiles_source AS
        SELECT
          b.steamid,
          COUNT(*) AS games_played,
          COUNT(DISTINCT b.map) AS maps_played,
          MIN(b.match_time) AS first_seen_at,
          MAX(b.match_time) AS last_seen_at,
          ROUND(AVG(CAST(b.won_game AS DOUBLE)), 4) AS career_win_rate,
          ROUND(SUM(b.kills) / NULLIF(SUM(b.deaths), 0), 4) AS career_kd_ratio,
          ROUND(AVG(b.kda_ratio), 4) AS career_avg_kda_ratio,
          ROUND(AVG(b.kills), 3) AS career_avg_kills,
          ROUND(AVG(b.damage_dealt), 3) AS career_avg_damage,
          ROUND(AVG(b.damage_per_minute), 3) AS career_avg_damage_per_minute,
          ROUND(AVG(b.impact_index), 4) AS career_avg_impact,
          ROUND(AVG(CAST(b.possible_tilt_label AS DOUBLE)), 4) AS tilt_risk_rate,
          lf.latest_match_time,
          ROUND(lf.rolling_5_avg_kills, 3) AS rolling_5_avg_kills,
          ROUND(lf.rolling_10_avg_damage, 3) AS rolling_10_avg_damage,
          ROUND(lf.rolling_10_avg_impact, 4) AS rolling_10_avg_impact,
          ROUND(lf.rolling_10_kda_ratio, 4) AS rolling_10_kda_ratio,
          ROUND(lf.rolling_10_win_rate, 4) AS rolling_10_win_rate,
          ROUND(lf.rolling_10_negative_chat_ratio, 4) AS rolling_10_negative_chat_ratio,
          ROUND(lf.form_delta_kills, 3) AS form_delta_kills,
          ROUND(lf.form_delta_damage, 3) AS form_delta_damage,
          ROUND(lf.form_delta_impact, 4) AS form_delta_impact,
          lf.momentum_label,
          ROUND(r30.recent_30_avg_kills, 3) AS recent_30_avg_kills,
          ROUND(r30.recent_30_avg_damage, 3) AS recent_30_avg_damage,
          ROUND(r30.recent_30_avg_impact, 4) AS recent_30_avg_impact,
          ROUND(r30.recent_30_win_rate, 4) AS recent_30_win_rate,
          CURRENT_TIMESTAMP() AS updated_at
        FROM tf2.default.features_player_match b
        JOIN changed_players cp ON cp.steamid = b.steamid
        LEFT JOIN latest_form lf
          ON lf.steamid = b.steamid
         AND lf.rn = 1
        LEFT JOIN recent_30 r30
          ON r30.steamid = b.steamid
        GROUP BY
          b.steamid,
          lf.latest_match_time,
          lf.rolling_5_avg_kills,
          lf.rolling_10_avg_damage,
          lf.rolling_10_avg_impact,
          lf.rolling_10_kda_ratio,
          lf.rolling_10_win_rate,
          lf.rolling_10_negative_chat_ratio,
          lf.form_delta_kills,
          lf.form_delta_damage,
          lf.form_delta_impact,
          lf.momentum_label,
          r30.recent_30_avg_kills,
          r30.recent_30_avg_damage,
          r30.recent_30_avg_impact,
          r30.recent_30_win_rate
        """
    )

    exists = table_exists(spark, SERVING_PLAYER_PROFILES_TABLE)
    full_rebuild = mode == "full" or not exists

    if full_rebuild:
        spark.sql(f"DROP TABLE IF EXISTS {SERVING_PLAYER_PROFILES_TABLE}")
        spark.sql(
            f"""
            CREATE TABLE {SERVING_PLAYER_PROFILES_TABLE}
            USING iceberg
            AS
            SELECT * FROM serving_player_profiles_source
            """
        )
        return

    spark.sql(
        f"""
        DELETE FROM {SERVING_PLAYER_PROFILES_TABLE}
        WHERE steamid IN (SELECT steamid FROM changed_players)
        """
    )
    spark.sql(
        f"""
        INSERT INTO {SERVING_PLAYER_PROFILES_TABLE}
        SELECT * FROM serving_player_profiles_source
        """
    )


def _create_map_bounds_view(spark: SparkSession, mode: str, refresh_days: int) -> None:
    if mode == "full":
        spark.sql(
            """
            CREATE OR REPLACE TEMP VIEW map_bounds AS
            SELECT CAST('1970-01-01' AS DATE) AS refresh_start_date
            """
        )
        return

    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW map_bounds AS
        SELECT COALESCE(
          DATE_SUB(MAX(CAST(TO_TIMESTAMP(sourcedateiso) AS DATE)), {refresh_days}),
          CAST('1970-01-01' AS DATE)
        ) AS refresh_start_date
        FROM tf2.default.logs
        """
    )


def refresh_serving_map_overview_daily(spark: SparkSession, mode: str, refresh_days: int) -> None:
    _create_map_bounds_view(spark, mode, refresh_days)

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW game_base AS
        SELECT
          l.logid,
          l.map,
          CAST(TO_TIMESTAMP(l.sourcedateiso) AS DATE) AS match_date,
          COALESCE(l.durationseconds, 0) AS duration_seconds,
          ABS(COALESCE(l.redscore, 0) - COALESCE(l.bluescore, 0)) AS score_delta
        FROM tf2.default.logs l
        CROSS JOIN map_bounds b
        WHERE CAST(TO_TIMESTAMP(l.sourcedateiso) AS DATE) >= b.refresh_start_date
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW kills_by_game AS
        SELECT
          s.logid,
          SUM(COALESCE(s.kills, 0)) AS total_kills
        FROM tf2.default.summaries s
        JOIN game_base gb ON gb.logid = s.logid
        GROUP BY s.logid
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW map_games AS
        SELECT
          gb.map,
          gb.match_date,
          COUNT(*) AS games,
          AVG(gb.duration_seconds) / 60.0 AS avg_duration_minutes,
          AVG(COALESCE(kbg.total_kills, 0)) AS avg_total_kills,
          AVG(COALESCE(kbg.total_kills, 0) / NULLIF(gb.duration_seconds / 60.0, 0)) AS avg_kills_per_minute,
          AVG(CASE WHEN gb.score_delta <= 1 THEN 1.0 ELSE 0.0 END) AS close_game_rate,
          AVG(CASE WHEN gb.score_delta >= 4 THEN 1.0 ELSE 0.0 END) AS blowout_rate
        FROM game_base gb
        LEFT JOIN kills_by_game kbg ON kbg.logid = gb.logid
        GROUP BY gb.map, gb.match_date
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW map_player_features AS
        SELECT
          fpm.map,
          fpm.match_date,
          AVG(fpm.impact_index) AS avg_player_impact_index,
          AVG(fpm.negative_chat_ratio) AS avg_negative_chat_ratio,
          AVG(CAST(fpm.possible_tilt_label AS DOUBLE)) AS tilt_signal_rate,
          approx_count_distinct(fpm.steamid) AS active_players
        FROM tf2.default.features_player_match fpm
        CROSS JOIN map_bounds b
        WHERE fpm.match_date >= b.refresh_start_date
        GROUP BY fpm.map, fpm.match_date
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW serving_map_overview_daily_source AS
        SELECT
          mg.map,
          mg.match_date,
          mg.games,
          ROUND(mg.avg_duration_minutes, 3) AS avg_duration_minutes,
          ROUND(mg.avg_total_kills, 3) AS avg_total_kills,
          ROUND(mg.avg_kills_per_minute, 3) AS avg_kills_per_minute,
          ROUND(mg.close_game_rate, 4) AS close_game_rate,
          ROUND(mg.blowout_rate, 4) AS blowout_rate,
          COALESCE(mpf.active_players, 0) AS active_players,
          ROUND(COALESCE(mpf.avg_player_impact_index, 0.0), 4) AS avg_player_impact_index,
          ROUND(COALESCE(mpf.avg_negative_chat_ratio, 0.0), 4) AS avg_negative_chat_ratio,
          ROUND(COALESCE(mpf.tilt_signal_rate, 0.0), 4) AS tilt_signal_rate,
          CURRENT_TIMESTAMP() AS updated_at
        FROM map_games mg
        LEFT JOIN map_player_features mpf
          ON mpf.map = mg.map
         AND mpf.match_date = mg.match_date
        """
    )

    exists = table_exists(spark, SERVING_MAP_OVERVIEW_TABLE)
    full_rebuild = mode == "full" or not exists

    if full_rebuild:
        spark.sql(f"DROP TABLE IF EXISTS {SERVING_MAP_OVERVIEW_TABLE}")
        spark.sql(
            f"""
            CREATE TABLE {SERVING_MAP_OVERVIEW_TABLE}
            USING iceberg
            PARTITIONED BY (months(match_date))
            AS
            SELECT * FROM serving_map_overview_daily_source
            """
        )
        return

    spark.sql(
        f"""
        DELETE FROM {SERVING_MAP_OVERVIEW_TABLE}
        WHERE match_date >= (SELECT refresh_start_date FROM map_bounds)
        """
    )
    spark.sql(
        f"""
        INSERT INTO {SERVING_MAP_OVERVIEW_TABLE}
        SELECT * FROM serving_map_overview_daily_source
        """
    )


def _create_deep_dive_bounds_view(spark: SparkSession, mode: str, refresh_days: int) -> None:
    if mode == "full":
        spark.sql(
            """
            CREATE OR REPLACE TEMP VIEW deep_dive_bounds AS
            SELECT CAST('1970-01-01' AS DATE) AS refresh_start_date
            """
        )
        return

    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW deep_dive_bounds AS
        SELECT COALESCE(
          DATE_SUB(MAX(match_date), {refresh_days}),
          CAST('1970-01-01' AS DATE)
        ) AS refresh_start_date
        FROM tf2.default.features_player_match
        """
    )


def refresh_serving_player_match_deep_dive(spark: SparkSession, mode: str, refresh_days: int) -> None:
    _create_deep_dive_bounds_view(spark, mode, refresh_days)

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW deep_dive_base AS
        SELECT
          fpm.logid,
          fpm.steamid,
          fpm.match_time,
          fpm.match_date,
          fpm.map,
          fpm.team,
          fpm.won_game,
          fpm.duration_seconds,
          fpm.kills,
          fpm.assists,
          fpm.deaths,
          fpm.damage_dealt,
          fpm.healing_done,
          fpm.ubers_used,
          fpm.classes_played_count,
          fpm.kill_share_of_team,
          fpm.damage_share_of_team,
          fpm.healing_share_of_team,
          fpm.impact_index,
          fpm.damage_per_minute,
          fpm.kda_ratio,
          fpm.chat_messages,
          fpm.avg_message_length,
          fpm.all_caps_messages,
          fpm.intense_punctuation_messages,
          fpm.negative_lexicon_hits,
          fpm.negative_chat_ratio,
          fpm.possible_tilt_label
        FROM (
          SELECT
            fpm.*,
            ROW_NUMBER() OVER (PARTITION BY fpm.logid, fpm.steamid ORDER BY fpm.match_time DESC) AS rn
          FROM tf2.default.features_player_match fpm
        ) fpm
        CROSS JOIN deep_dive_bounds b
        WHERE fpm.rn = 1
          AND fpm.match_date >= b.refresh_start_date
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW deep_dive_form AS
        SELECT
          frf.logid,
          frf.steamid,
          frf.games_played_to_date,
          frf.rolling_5_avg_kills,
          frf.rolling_10_avg_damage,
          frf.rolling_10_avg_impact,
          frf.rolling_10_kda_ratio,
          frf.rolling_10_win_rate,
          frf.rolling_10_negative_chat_ratio,
          frf.form_delta_kills,
          frf.form_delta_damage,
          frf.form_delta_impact,
          frf.momentum_label
        FROM (
          SELECT
            frf.*,
            ROW_NUMBER() OVER (PARTITION BY frf.logid, frf.steamid ORDER BY frf.match_time DESC) AS rn
          FROM tf2.default.features_player_recent_form frf
          JOIN deep_dive_base rb
            ON rb.logid = frf.logid
           AND rb.steamid = frf.steamid
        ) frf
        WHERE frf.rn = 1
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW serving_player_match_deep_dive_source AS
        SELECT
          rb.logid,
          rb.steamid,
          rb.match_time,
          rb.match_date,
          rb.map,
          rb.team,
          rb.won_game,
          rb.duration_seconds,
          rb.kills,
          rb.assists,
          rb.deaths,
          rb.damage_dealt,
          rb.healing_done,
          rb.ubers_used,
          rb.classes_played_count,
          ROUND(rb.kill_share_of_team, 4) AS kill_share_of_team,
          ROUND(rb.damage_share_of_team, 4) AS damage_share_of_team,
          ROUND(rb.healing_share_of_team, 4) AS healing_share_of_team,
          ROUND(rb.impact_index, 4) AS impact_index,
          ROUND(rb.damage_per_minute, 3) AS damage_per_minute,
          ROUND(rb.kda_ratio, 3) AS kda_ratio,
          rb.chat_messages,
          ROUND(rb.avg_message_length, 3) AS avg_message_length,
          rb.all_caps_messages,
          rb.intense_punctuation_messages,
          rb.negative_lexicon_hits,
          ROUND(rb.negative_chat_ratio, 4) AS negative_chat_ratio,
          rb.possible_tilt_label,
          frf.games_played_to_date,
          ROUND(frf.rolling_5_avg_kills, 3) AS rolling_5_avg_kills,
          ROUND(frf.rolling_10_avg_damage, 3) AS rolling_10_avg_damage,
          ROUND(frf.rolling_10_avg_impact, 4) AS rolling_10_avg_impact,
          ROUND(frf.rolling_10_kda_ratio, 4) AS rolling_10_kda_ratio,
          ROUND(frf.rolling_10_win_rate, 4) AS rolling_10_win_rate,
          ROUND(frf.rolling_10_negative_chat_ratio, 4) AS rolling_10_negative_chat_ratio,
          ROUND(frf.form_delta_kills, 3) AS form_delta_kills,
          ROUND(frf.form_delta_damage, 3) AS form_delta_damage,
          ROUND(frf.form_delta_impact, 4) AS form_delta_impact,
          COALESCE(frf.momentum_label, 'unknown') AS momentum_label,
          CASE
            WHEN COALESCE(rb.possible_tilt_label, 0) = 1 OR COALESCE(rb.negative_chat_ratio, 0.0) >= 0.1800 THEN 'high'
            WHEN COALESCE(rb.negative_chat_ratio, 0.0) >= 0.0800 THEN 'medium'
            ELSE 'low'
          END AS behaviour_risk_tier,
          CASE
            WHEN COALESCE(rb.impact_index, 0.0) >= 0.2400 THEN 'elite'
            WHEN COALESCE(rb.impact_index, 0.0) >= 0.1800 THEN 'strong'
            WHEN COALESCE(rb.impact_index, 0.0) >= 0.1300 THEN 'average'
            ELSE 'struggling'
          END AS impact_tier,
          CURRENT_TIMESTAMP() AS updated_at
        FROM deep_dive_base rb
        LEFT JOIN deep_dive_form frf
          ON frf.logid = rb.logid
         AND frf.steamid = rb.steamid
        """
    )

    exists = table_exists(spark, SERVING_DEEP_DIVE_TABLE)
    full_rebuild = mode == "full" or not exists

    if full_rebuild:
        spark.sql(f"DROP TABLE IF EXISTS {SERVING_DEEP_DIVE_TABLE}")
        spark.sql(
            f"""
            CREATE TABLE {SERVING_DEEP_DIVE_TABLE}
            USING iceberg
            PARTITIONED BY (months(match_date))
            AS
            SELECT * FROM serving_player_match_deep_dive_source
            """
        )
        return

    spark.sql(
        f"""
        DELETE FROM {SERVING_DEEP_DIVE_TABLE}
        WHERE match_date >= (SELECT refresh_start_date FROM deep_dive_bounds)
        """
    )
    spark.sql(
        f"""
        INSERT INTO {SERVING_DEEP_DIVE_TABLE}
        SELECT * FROM serving_player_match_deep_dive_source
        """
    )

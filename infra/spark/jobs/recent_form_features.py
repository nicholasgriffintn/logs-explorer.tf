from __future__ import annotations

from pyspark.sql import SparkSession

from spark_utils import FEATURES_RECENT_FORM_TABLE, table_exists


def create_recent_form_source_view(spark: SparkSession, mode: str, refresh_days: int) -> None:
    if mode == "full":
        spark.sql(
            """
            CREATE OR REPLACE TEMP VIEW changed_players AS
            SELECT DISTINCT steamid
            FROM tf2.default.features_player_match
            """
        )
    else:
        spark.sql(
            f"""
            CREATE OR REPLACE TEMP VIEW feature_bounds AS
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
            CROSS JOIN feature_bounds b
            WHERE fpm.match_date >= b.refresh_start_date
            """
        )

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW recent_form_scored AS
        SELECT
          steamid,
          logid,
          map,
          team,
          match_time,
          match_date,
          kills,
          assists,
          deaths,
          damage_dealt,
          healing_done,
          impact_index,
          negative_chat_ratio,
          won_game,
          ROW_NUMBER() OVER (PARTITION BY steamid ORDER BY match_time) AS games_played_to_date,
          AVG(kills) OVER (
            PARTITION BY steamid
            ORDER BY match_time
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
          ) AS rolling_5_avg_kills,
          AVG(damage_dealt) OVER (
            PARTITION BY steamid
            ORDER BY match_time
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
          ) AS rolling_10_avg_damage,
          AVG(impact_index) OVER (
            PARTITION BY steamid
            ORDER BY match_time
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
          ) AS rolling_10_avg_impact,
          AVG(
            (kills + assists) / NULLIF(CAST(deaths AS DOUBLE), 0)
          ) OVER (
            PARTITION BY steamid
            ORDER BY match_time
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
          ) AS rolling_10_kda_ratio,
          AVG(CAST(won_game AS DOUBLE)) OVER (
            PARTITION BY steamid
            ORDER BY match_time
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
          ) AS rolling_10_win_rate,
          AVG(negative_chat_ratio) OVER (
            PARTITION BY steamid
            ORDER BY match_time
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
          ) AS rolling_10_negative_chat_ratio,
          AVG(kills) OVER (PARTITION BY steamid) AS career_avg_kills,
          AVG(damage_dealt) OVER (PARTITION BY steamid) AS career_avg_damage,
          AVG(impact_index) OVER (PARTITION BY steamid) AS career_avg_impact
        FROM tf2.default.features_player_match
        WHERE steamid IN (SELECT steamid FROM changed_players)
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW features_player_recent_form_source AS
        SELECT
          steamid,
          logid,
          map,
          team,
          match_time,
          match_date,
          games_played_to_date,
          kills,
          assists,
          deaths,
          damage_dealt,
          healing_done,
          ROUND(impact_index, 4) AS impact_index,
          ROUND(negative_chat_ratio, 4) AS negative_chat_ratio,
          won_game,
          ROUND(rolling_5_avg_kills, 3) AS rolling_5_avg_kills,
          ROUND(rolling_10_avg_damage, 3) AS rolling_10_avg_damage,
          ROUND(rolling_10_avg_impact, 4) AS rolling_10_avg_impact,
          ROUND(rolling_10_kda_ratio, 4) AS rolling_10_kda_ratio,
          ROUND(rolling_10_win_rate, 4) AS rolling_10_win_rate,
          ROUND(rolling_10_negative_chat_ratio, 4) AS rolling_10_negative_chat_ratio,
          ROUND(career_avg_kills, 3) AS career_avg_kills,
          ROUND(career_avg_damage, 3) AS career_avg_damage,
          ROUND(career_avg_impact, 4) AS career_avg_impact,
          ROUND(rolling_5_avg_kills - career_avg_kills, 3) AS form_delta_kills,
          ROUND(rolling_10_avg_damage - career_avg_damage, 3) AS form_delta_damage,
          ROUND(rolling_10_avg_impact - career_avg_impact, 4) AS form_delta_impact,
          CASE
            WHEN (rolling_10_avg_impact - career_avg_impact) >= 0.016 THEN 'hot'
            WHEN (rolling_10_avg_impact - career_avg_impact) <= -0.020 THEN 'cold'
            ELSE 'stable'
          END AS momentum_label
        FROM recent_form_scored
        """
    )


def refresh_features_player_recent_form(spark: SparkSession, mode: str) -> None:
    exists = table_exists(spark, FEATURES_RECENT_FORM_TABLE)
    full_rebuild = mode == "full" or not exists

    if full_rebuild:
        spark.sql(f"DROP TABLE IF EXISTS {FEATURES_RECENT_FORM_TABLE}")
        spark.sql(
            f"""
            CREATE TABLE {FEATURES_RECENT_FORM_TABLE}
            USING iceberg
            PARTITIONED BY (months(match_date))
            AS
            SELECT * FROM features_player_recent_form_source
            """
        )
        return

    spark.sql(
        f"""
        DELETE FROM {FEATURES_RECENT_FORM_TABLE}
        WHERE steamid IN (SELECT steamid FROM changed_players)
        """
    )
    spark.sql(
        f"""
        INSERT INTO {FEATURES_RECENT_FORM_TABLE}
        SELECT * FROM features_player_recent_form_source
        """
    )

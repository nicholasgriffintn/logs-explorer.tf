from __future__ import annotations

from pyspark.sql import SparkSession

from ops.spark_utils import FEATURES_MATCH_TABLE, table_exists


def create_match_source_view(spark: SparkSession) -> None:
    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW team_totals AS
        SELECT
          s.logid,
          s.team,
          SUM(COALESCE(s.kills, 0)) AS team_kills,
          SUM(COALESCE(s.damagedealt, 0)) AS team_damage,
          SUM(COALESCE(s.healingdone, 0)) AS team_healing
        FROM summaries_base s
        JOIN changed_logs cl ON cl.logid = s.logid
        GROUP BY s.logid, s.team
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW chat_by_player_game AS
        SELECT
          m.steamid,
          m.logid,
          COUNT(*) AS chat_messages,
          AVG(LENGTH(m.message)) AS avg_message_length,
          SUM(CASE WHEN m.message RLIKE '^[A-Z0-9 !?.''-]+$' THEN 1 ELSE 0 END) AS all_caps_messages,
          SUM(CASE WHEN m.messagelower RLIKE '!{2,}' THEN 1 ELSE 0 END) AS intense_punctuation_messages,
          SUM(
            CASE
              WHEN LOWER(m.message) RLIKE '(^|[^a-z0-9])(noob|trash|idiot|stupid|cheat|cheater|ez|wtf|losing|throw|threw|report)([^a-z0-9]|$)'
              THEN 1
              ELSE 0
            END
          ) AS negative_lexicon_hits
        FROM messages_base m
        JOIN changed_logs cl ON cl.logid = m.logid
        WHERE m.steamid IS NOT NULL
        GROUP BY m.steamid, m.logid
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW features_player_match_source AS
        SELECT
          s.logid,
          s.steamid,
          s.map,
          s.team,
          CAST(TO_TIMESTAMP(s.sourcedateiso) AS TIMESTAMP) AS match_time,
          CAST(TO_TIMESTAMP(s.sourcedateiso) AS DATE) AS match_date,
          COALESCE(l.durationseconds, 0) AS duration_seconds,
          COALESCE(s.kills, 0) AS kills,
          COALESCE(s.assists, 0) AS assists,
          COALESCE(s.deaths, 0) AS deaths,
          COALESCE(s.damagedealt, 0) AS damage_dealt,
          COALESCE(s.healingdone, 0) AS healing_done,
          COALESCE(s.ubersused, 0) AS ubers_used,
          COALESCE(s.classesplayedcsv, '') AS classes_played_csv,
          SIZE(
            FILTER(
              TRANSFORM(SPLIT(COALESCE(s.classesplayedcsv, ''), ','), c -> TRIM(c)),
              c -> c <> ''
            )
          ) AS classes_played_count,
          CASE
            WHEN s.team = 'Red' AND l.redscore > l.bluescore THEN 1
            WHEN s.team = 'Blue' AND l.bluescore > l.redscore THEN 1
            ELSE 0
          END AS won_game,
          ROUND(COALESCE(s.kills, 0) / NULLIF(CAST(tt.team_kills AS DOUBLE), 0), 4) AS kill_share_of_team,
          ROUND(COALESCE(s.damagedealt, 0) / NULLIF(CAST(tt.team_damage AS DOUBLE), 0), 4) AS damage_share_of_team,
          ROUND(COALESCE(s.healingdone, 0) / NULLIF(CAST(tt.team_healing AS DOUBLE), 0), 4) AS healing_share_of_team,
          ROUND(
            0.45 * (COALESCE(s.damagedealt, 0) / NULLIF(CAST(tt.team_damage AS DOUBLE), 0))
              + 0.35 * (COALESCE(s.kills, 0) / NULLIF(CAST(tt.team_kills AS DOUBLE), 0))
              + 0.20 * (COALESCE(s.healingdone, 0) / NULLIF(CAST(tt.team_healing AS DOUBLE), 0)),
            4
          ) AS impact_index,
          ROUND(
            COALESCE(s.damagedealt, 0) / NULLIF(COALESCE(l.durationseconds, 0) / 60.0, 0),
            3
          ) AS damage_per_minute,
          ROUND(
            (COALESCE(s.kills, 0) + COALESCE(s.assists, 0))
            / NULLIF(CAST(COALESCE(s.deaths, 0) AS DOUBLE), 0),
            3
          ) AS kda_ratio,
          COALESCE(cbpg.chat_messages, 0) AS chat_messages,
          ROUND(COALESCE(cbpg.avg_message_length, 0.0), 3) AS avg_message_length,
          COALESCE(cbpg.all_caps_messages, 0) AS all_caps_messages,
          COALESCE(cbpg.intense_punctuation_messages, 0) AS intense_punctuation_messages,
          COALESCE(cbpg.negative_lexicon_hits, 0) AS negative_lexicon_hits,
          CASE
            WHEN COALESCE(cbpg.chat_messages, 0) = 0 THEN 0.0
            ELSE CAST(COALESCE(cbpg.negative_lexicon_hits, 0) AS DOUBLE) / cbpg.chat_messages
          END AS negative_chat_ratio,
          CASE
            WHEN COALESCE(s.deaths, 0) >= 12
              AND COALESCE(cbpg.chat_messages, 0) >= 2
              AND COALESCE(cbpg.negative_lexicon_hits, 0) >= 1 THEN 1
            ELSE 0
          END AS possible_tilt_label
        FROM summaries_base s
        JOIN changed_logs cl ON cl.logid = s.logid
        LEFT JOIN logs_base l ON l.logid = s.logid
        LEFT JOIN team_totals tt
          ON tt.logid = s.logid
         AND tt.team = s.team
        LEFT JOIN chat_by_player_game cbpg
          ON cbpg.logid = s.logid
         AND cbpg.steamid = s.steamid
        """
    )


def refresh_features_player_match(spark: SparkSession, mode: str) -> None:
    exists = table_exists(spark, FEATURES_MATCH_TABLE)
    full_rebuild = mode == "full" or not exists

    if full_rebuild:
        spark.sql(f"DROP TABLE IF EXISTS {FEATURES_MATCH_TABLE}")
        spark.sql(
            f"""
            CREATE TABLE {FEATURES_MATCH_TABLE}
            USING iceberg
            PARTITIONED BY (months(match_date))
            AS
            SELECT * FROM features_player_match_source
            """
        )
        return

    spark.sql(
        f"""
        DELETE FROM {FEATURES_MATCH_TABLE}
        WHERE logid IN (SELECT logid FROM changed_logs)
        """
    )
    spark.sql(
        f"""
        INSERT INTO {FEATURES_MATCH_TABLE}
        SELECT * FROM features_player_match_source
        """
    )

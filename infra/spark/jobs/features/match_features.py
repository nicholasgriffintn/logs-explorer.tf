from __future__ import annotations

from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ops.spark_utils import FEATURES_MATCH_TABLE, run_delete_insert_with_retry, table_exists

SENTIMENT_PIPELINE_NAME = "analyze_sentimentdl_use_twitter"
SENTIMENT_REGEX_FALLBACK = r"(?i)\b(?:trash|garbage|stupid|idiot|noob|useless|terrible|awful|worst|kys|report)\b"


def _cpu_supports_avx() -> bool:
    cpuinfo_path = Path("/proc/cpuinfo")
    if not cpuinfo_path.exists():
        return True

    try:
        cpuinfo = cpuinfo_path.read_text(encoding="utf-8", errors="ignore").lower()
    except OSError:
        return True

    for line in cpuinfo.splitlines():
        if line.startswith("flags") or line.startswith("features"):
            _, _, flags_text = line.partition(":")
            if "avx" in flags_text.split():
                return True
    return False


def _score_messages_with_sentimentdl(chat_messages):
    from sparknlp.pretrained import PretrainedPipeline

    sentiment_pipeline = PretrainedPipeline(SENTIMENT_PIPELINE_NAME, lang="en")
    scored_messages = sentiment_pipeline.model.transform(chat_messages.withColumn("text", F.col("chat_text")))
    return scored_messages.withColumn("sentiment_label", F.lower(F.expr("sentiment[0].result")))


def _score_messages_with_vivekn(chat_messages):
    from sparknlp.annotator import Normalizer, Tokenizer, ViveknSentimentModel
    from sparknlp.base import DocumentAssembler

    document = DocumentAssembler().setInputCol("text").setOutputCol("document")
    token = Tokenizer().setInputCols(["document"]).setOutputCol("token")
    normalizer = Normalizer().setInputCols(["token"]).setOutputCol("normal")
    vivekn = (
        ViveknSentimentModel.pretrained("sentiment_vivekn", "en")
        .setInputCols(["document", "normal"])
        .setOutputCol("sentiment")
    )

    with_text = chat_messages.withColumn("text", F.col("chat_text"))
    sentiment_pipeline = Pipeline(stages=[document, token, normalizer, vivekn])
    bootstrap_df = chat_messages.sparkSession.createDataFrame([("",)], ["text"])
    scored_messages = sentiment_pipeline.fit(bootstrap_df).transform(with_text)
    return scored_messages.withColumn("sentiment_label", F.lower(F.expr("sentiment[0].result")))


def create_chat_by_player_game_view(spark: SparkSession) -> None:
    chat_messages = spark.sql(
        """
        SELECT
          m.steamid,
          m.logid,
          COALESCE(m.message, '') AS chat_text
        FROM messages_base m
        JOIN changed_logs cl ON cl.logid = m.logid
        WHERE m.steamid IS NOT NULL
        """
    )

    with_sentiment = None
    if _cpu_supports_avx():
        try:
            print(f"Using Spark NLP sentiment pipeline: {SENTIMENT_PIPELINE_NAME}")
            with_sentiment = _score_messages_with_sentimentdl(chat_messages)
        except Exception as exc:
            print(f"SentimentDL unavailable ({exc}); falling back to Vivekn sentiment model")
    else:
        print("AVX not detected; using Vivekn sentiment model fallback")

    if with_sentiment is None:
        try:
            with_sentiment = _score_messages_with_vivekn(chat_messages)
        except Exception as exc:
            print(f"Vivekn sentiment model unavailable ({exc}); falling back to regex sentiment")
            with_sentiment = chat_messages.withColumn(
                "sentiment_label",
                F.when(F.col("chat_text").rlike(SENTIMENT_REGEX_FALLBACK), F.lit("negative")).otherwise(F.lit("neutral")),
            )

    chat_by_player_game = with_sentiment.groupBy("steamid", "logid").agg(
        F.count(F.lit(1)).alias("chat_messages"),
        F.avg(F.length("chat_text")).alias("avg_message_length"),
        F.sum(F.when(F.col("chat_text").rlike(r"^[A-Z0-9 !?.'\-]+$"), 1).otherwise(0)).alias("all_caps_messages"),
        F.sum(F.when(F.col("chat_text").rlike(r"!{2,}"), 1).otherwise(0)).alias("intense_punctuation_messages"),
        F.sum(F.when(F.col("sentiment_label") == F.lit("negative"), 1).otherwise(0)).alias("negative_lexicon_hits"),
    )
    chat_by_player_game.createOrReplaceTempView("chat_by_player_game")


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

    create_chat_by_player_game_view(spark)

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

    run_delete_insert_with_retry(
        spark=spark,
        table_name=FEATURES_MATCH_TABLE,
        delete_sql=f"""
        DELETE FROM {FEATURES_MATCH_TABLE}
        WHERE logid IN (SELECT logid FROM changed_logs)
        """,
        insert_sql=f"""
        INSERT INTO {FEATURES_MATCH_TABLE}
        SELECT * FROM features_player_match_source
        """,
    )

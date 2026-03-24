from __future__ import annotations

from pyspark.sql import SparkSession


def create_base_views(spark: SparkSession) -> None:
    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW logs_by_record AS
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
        CREATE OR REPLACE TEMP VIEW logs_base AS
        SELECT * FROM (
          SELECT
            l.*,
            ROW_NUMBER() OVER (
              PARTITION BY l.logid
              ORDER BY l.__ingest_ts DESC, l.sourcedateepochseconds DESC
            ) AS rn_log
          FROM logs_by_record l
        ) ranked
        WHERE rn_log = 1
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW summaries_by_record AS
        SELECT * FROM (
          SELECT
            s.*,
            ROW_NUMBER() OVER (PARTITION BY s.recordid ORDER BY s.__ingest_ts DESC) AS rn
          FROM tf2.default.summaries s
        ) ranked
        WHERE rn = 1
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW summaries_base AS
        SELECT * FROM (
          SELECT
            s.*,
            ROW_NUMBER() OVER (
              PARTITION BY s.logid, s.steamid
              ORDER BY s.__ingest_ts DESC, s.sourcedateepochseconds DESC
            ) AS rn_player
          FROM summaries_by_record s
          WHERE s.team IN ('Red', 'Blue')
        ) ranked
        WHERE rn_player = 1
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW messages_base AS
        SELECT * FROM (
          SELECT
            m.*,
            ROW_NUMBER() OVER (PARTITION BY m.recordid ORDER BY m.__ingest_ts DESC) AS rn
          FROM tf2.default.messages m
        ) ranked
        WHERE rn = 1
        """
    )


def create_changed_logs_view(spark: SparkSession, mode: str, refresh_days: int) -> None:
    if mode == "full":
        spark.sql(
            """
            CREATE OR REPLACE TEMP VIEW changed_logs AS
            SELECT DISTINCT logid
            FROM summaries_base
            """
        )
        return

    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW bounds AS
        SELECT COALESCE(
          DATE_SUB(MAX(CAST(TO_TIMESTAMP(sourcedateiso) AS DATE)), {refresh_days}),
          CAST('1970-01-01' AS DATE)
        ) AS refresh_start_date
        FROM summaries_base
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW changed_logs AS
        SELECT DISTINCT s.logid
        FROM summaries_base s
        CROSS JOIN bounds b
        WHERE CAST(TO_TIMESTAMP(s.sourcedateiso) AS DATE) >= b.refresh_start_date
        """
    )

from __future__ import annotations

from pyspark.sql import SparkSession


FEATURES_MATCH_TABLE = "tf2.default.features_player_match"
FEATURES_RECENT_FORM_TABLE = "tf2.default.features_player_recent_form"


def table_exists(spark: SparkSession, table_name: str) -> bool:
    try:
        spark.sql(f"DESCRIBE TABLE {table_name}").collect()
        return True
    except Exception:
        return False


def print_counts(spark: SparkSession) -> None:
    counts = spark.sql(
        f"""
        SELECT '{FEATURES_MATCH_TABLE}' AS table_name, COUNT(*) AS row_count
        FROM {FEATURES_MATCH_TABLE}
        UNION ALL
        SELECT '{FEATURES_RECENT_FORM_TABLE}' AS table_name, COUNT(*) AS row_count
        FROM {FEATURES_RECENT_FORM_TABLE}
        """
    ).collect()

    print("Feature row counts:")
    for row in counts:
        print(f"  - {row['table_name']}: {row['row_count']}")

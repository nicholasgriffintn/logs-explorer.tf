from __future__ import annotations

import logging
import time

from pyspark.sql import SparkSession


FEATURES_MATCH_TABLE = "tf2.default.features_player_match"
FEATURES_RECENT_FORM_TABLE = "tf2.default.features_player_recent_form"
MAX_DELETE_INSERT_RETRIES = 3
DELETE_INSERT_RETRY_BACKOFF_SECONDS = 5

_RETRYABLE_ICEBERG_TOKENS = (
    "missing required files to delete",
    "found conflicting files",
)


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


def _is_retryable_iceberg_validation_error(exc: Exception) -> bool:
    message = str(exc).lower()
    if "validationexception" not in message:
        return False
    return any(token in message for token in _RETRYABLE_ICEBERG_TOKENS)


def run_delete_insert_with_retry(
    spark: SparkSession,
    table_name: str,
    delete_sql: str,
    insert_sql: str,
) -> None:
    logger = logging.getLogger(__name__)

    for attempt in range(1, MAX_DELETE_INSERT_RETRIES + 1):
        try:
            spark.sql(delete_sql)
            spark.sql(insert_sql)
            return
        except Exception as exc:
            if not _is_retryable_iceberg_validation_error(exc) or attempt == MAX_DELETE_INSERT_RETRIES:
                raise

            wait_seconds = DELETE_INSERT_RETRY_BACKOFF_SECONDS * attempt
            logger.warning(
                "Retrying Iceberg DELETE+INSERT for %s after validation conflict (attempt %s/%s, sleeping %ss): %s",
                table_name,
                attempt + 1,
                MAX_DELETE_INSERT_RETRIES,
                wait_seconds,
                exc,
            )
            time.sleep(wait_seconds)

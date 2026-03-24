from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import SparkSession


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _format_ts(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _escape_sql(value: str) -> str:
    return value.replace("'", "''")


def record_run(
    spark: SparkSession,
    run_id: str,
    run_mode: str,
    step_name: str,
    status: str,
    started_at: datetime,
    finished_at: datetime,
    duration_seconds: float,
    row_count: int | None = None,
    error_text: str | None = None,
) -> None:
    row_count_sql = "NULL" if row_count is None else str(int(row_count))
    error_sql = "NULL" if not error_text else f"'{_escape_sql(error_text)}'"
    started_sql = _format_ts(started_at)
    finished_sql = _format_ts(finished_at)

    spark.sql(
        f"""
        INSERT INTO tf2.default.ops_pipeline_runs
        VALUES (
          '{_escape_sql(run_id)}',
          '{_escape_sql(run_mode)}',
          '{_escape_sql(step_name)}',
          '{_escape_sql(status)}',
          TIMESTAMP('{started_sql}'),
          TIMESTAMP('{finished_sql}'),
          CAST({duration_seconds:.6f} AS DOUBLE),
          {row_count_sql},
          {error_sql},
          CURRENT_TIMESTAMP()
        )
        """
    )

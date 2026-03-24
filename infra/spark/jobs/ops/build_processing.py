#!/usr/bin/env python3

from __future__ import annotations

import argparse
import random
from dataclasses import dataclass
from typing import Callable

from pyspark.sql import SparkSession

from features.base_views import create_base_views, create_changed_logs_view
from features.match_features import create_match_source_view, refresh_features_player_match
from features.recent_form_features import create_recent_form_source_view, refresh_features_player_recent_form
from ml.ml_progress import refresh_ml_progress_serving_tables
from ml.ml_snapshot import build_training_snapshot
from ops.catalog_tables import ensure_core_pipeline_tables, ensure_ml_tables
from ops.pipeline_runs import record_run, utc_now
from serving.serving_tables import (
    refresh_serving_map_overview_daily,
    refresh_serving_player_match_deep_dive,
    refresh_serving_player_profiles,
)


@dataclass
class Step:
    name: str
    fn: Callable[[], None]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Spark data-processing pipeline.")
    parser.add_argument("--mode", choices=["full", "incremental"], default="incremental")
    parser.add_argument("--refresh-days", type=int, default=7)
    parser.add_argument(
        "--pipeline",
        choices=["feature-serving", "ml", "all"],
        default="all",
        help="Pipeline slice to execute.",
    )
    return parser.parse_args()


def run_step(spark: SparkSession, run_id: str, mode: str, step: Step) -> None:
    started_at = utc_now()
    error_text = None
    status = "success"
    row_count = None

    try:
        step.fn()
    except Exception as exc:  # pragma: no cover - runtime guard
        status = "failed"
        error_text = str(exc)
        raise
    finally:
        finished_at = utc_now()
        duration = (finished_at - started_at).total_seconds()
        record_run(
            spark=spark,
            run_id=run_id,
            run_mode=mode,
            step_name=step.name,
            status=status,
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=duration,
            row_count=row_count,
            error_text=error_text,
        )


def main() -> int:
    args = parse_args()
    pipeline_id = args.pipeline.replace("-", "_")
    spark = (
        SparkSession.builder.appName(f"tf2-processing-{pipeline_id}")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    run_id = f"run_{pipeline_id}_{utc_now().strftime('%Y%m%dT%H%M%SZ')}_{random.randint(1000, 99999)}"
    pipeline_started = utc_now()
    pipeline_status = "success"
    pipeline_error = None
    pipeline_step_name = f"pipeline_{pipeline_id}"

    ensure_core_pipeline_tables(spark)
    if args.pipeline in {"ml", "all"}:
        ensure_ml_tables(spark)

    def refresh_features() -> None:
        create_base_views(spark)
        create_changed_logs_view(spark, args.mode, args.refresh_days)
        create_match_source_view(spark)
        refresh_features_player_match(spark, args.mode)
        create_recent_form_source_view(spark, args.mode, args.refresh_days)
        refresh_features_player_recent_form(spark, args.mode)

    try:
        steps: list[Step] = []

        if args.pipeline in {"feature-serving", "all"}:
            steps.extend(
                [
                    Step(name="features_refresh", fn=refresh_features),
                    Step(
                        name="serving_player_profiles_refresh",
                        fn=lambda: refresh_serving_player_profiles(spark, args.mode, args.refresh_days),
                    ),
                    Step(
                        name="serving_map_overview_daily_refresh",
                        fn=lambda: refresh_serving_map_overview_daily(spark, args.mode, args.refresh_days),
                    ),
                    Step(
                        name="serving_player_match_deep_dive_refresh",
                        fn=lambda: refresh_serving_player_match_deep_dive(spark, args.mode, args.refresh_days),
                    ),
                ]
            )

        if args.pipeline in {"ml", "all"}:
            steps.extend(
                [
                    Step(name="ml_training_snapshot_refresh", fn=lambda: build_training_snapshot(spark)),
                    Step(name="serving_ml_progress_refresh", fn=lambda: refresh_ml_progress_serving_tables(spark)),
                ]
            )

        for step in steps:
            run_step(spark, run_id, args.mode, step)
    except Exception as exc:  # pragma: no cover - runtime guard
        pipeline_status = "failed"
        pipeline_error = str(exc)
        raise
    finally:
        pipeline_finished = utc_now()
        pipeline_duration = (pipeline_finished - pipeline_started).total_seconds()
        record_run(
            spark=spark,
            run_id=run_id,
            run_mode=args.mode,
            step_name=pipeline_step_name,
            status=pipeline_status,
            started_at=pipeline_started,
            finished_at=pipeline_finished,
            duration_seconds=pipeline_duration,
            row_count=None,
            error_text=pipeline_error,
        )
        spark.stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

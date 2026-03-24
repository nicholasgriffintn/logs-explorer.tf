#!/usr/bin/env python3

from __future__ import annotations

import argparse
import sys

from pyspark.sql import SparkSession

from base_views import create_base_views, create_changed_logs_view
from match_features import create_match_source_view, refresh_features_player_match
from recent_form_features import create_recent_form_source_view, refresh_features_player_recent_form
from spark_utils import print_counts


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build TF2 feature tables with Spark.")
    parser.add_argument("--mode", choices=["full", "incremental"], default="incremental")
    parser.add_argument("--refresh-days", type=int, default=7)
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)

    spark = (
        SparkSession.builder.appName("tf2-feature-pipeline")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    try:
        print(f"Running feature pipeline mode={args.mode}, refresh_days={args.refresh_days}")
        create_base_views(spark)
        create_changed_logs_view(spark, args.mode, args.refresh_days)
        create_match_source_view(spark)
        refresh_features_player_match(spark, args.mode)
        create_recent_form_source_view(spark, args.mode, args.refresh_days)
        refresh_features_player_recent_form(spark, args.mode)
        print_counts(spark)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

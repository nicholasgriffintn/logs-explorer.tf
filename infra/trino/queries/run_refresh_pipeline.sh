#!/usr/bin/env bash

set -euo pipefail

MODE="${1:-incremental}"
TRINO_CONTAINER="${TRINO_CONTAINER:-tf2-trino}"
QUERIES_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_ID="${RUN_ID:-run_$(date -u +%Y%m%dT%H%M%SZ)_$RANDOM}"
PIPELINE_STATUS="success"

usage() {
  cat <<USAGE
Usage: $(basename "$0") [incremental|full]

Environment variables:
  TRINO_CONTAINER  Docker container name for Trino (default: tf2-trino)
  RUN_ID           Optional custom run identifier
USAGE
}

if [[ "$MODE" != "incremental" && "$MODE" != "full" ]]; then
  usage
  exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required" >&2
  exit 1
fi

if ! docker ps --format '{{.Names}}' | grep -q "^${TRINO_CONTAINER}$"; then
  echo "Trino container '${TRINO_CONTAINER}' is not running." >&2
  echo "Start it with: docker compose -f infra/trino/docker-compose.yml up -d" >&2
  exit 1
fi

now_iso() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

escape_sql() {
  printf "%s" "$1" | sed "s/'/''/g"
}

run_sql_file() {
  local sql_file="$1"
  docker exec -i "$TRINO_CONTAINER" trino < "$sql_file"
}

run_sql_inline() {
  local sql="$1"
  printf '%s\n' "$sql" | docker exec -i "$TRINO_CONTAINER" trino
}

query_scalar() {
  local sql="$1"
  docker exec -i "$TRINO_CONTAINER" trino --output-format CSV_UNQUOTED --execute "$sql" \
    | tail -n 1 \
    | tr -d '\r'
}

record_run() {
  local step_name="$1"
  local status="$2"
  local started_iso="$3"
  local finished_iso="$4"
  local duration_seconds="$5"
  local row_count="$6"
  local error_text="$7"

  local row_count_sql="NULL"
  local error_sql="NULL"

  if [[ -n "$row_count" && "$row_count" != "NULL" ]]; then
    row_count_sql="$row_count"
  fi

  if [[ -n "$error_text" ]]; then
    error_sql="'$(escape_sql "$error_text")'"
  fi

  run_sql_inline "
INSERT INTO tf2.default.ops_pipeline_runs (
  run_id,
  run_mode,
  step_name,
  status,
  started_at,
  finished_at,
  duration_seconds,
  row_count,
  error_text,
  created_at
)
VALUES (
  '${RUN_ID}',
  '${MODE}',
  '${step_name}',
  '${status}',
  from_iso8601_timestamp('${started_iso}'),
  from_iso8601_timestamp('${finished_iso}'),
  CAST(${duration_seconds} AS DOUBLE),
  ${row_count_sql},
  ${error_sql},
  CURRENT_TIMESTAMP
);
"
}

run_step_file() {
  local step_name="$1"
  local sql_file="$2"
  local row_count_query="$3"

  local started_iso
  local finished_iso
  local start_epoch
  local end_epoch
  local duration
  local status="success"
  local error_text=""
  local row_count="NULL"

  started_iso="$(now_iso)"
  start_epoch="$(date -u +%s)"

  echo "-> Running ${step_name} (${sql_file##*/})"

  set +e
  docker exec -i "$TRINO_CONTAINER" trino < "$sql_file"
  local step_exit=$?
  set -e

  if [[ $step_exit -ne 0 ]]; then
    status="failed"
    error_text="SQL execution failed for ${sql_file##*/}"
    PIPELINE_STATUS="failed"
  elif [[ -n "$row_count_query" ]]; then
    set +e
    row_count="$(query_scalar "$row_count_query")"
    local row_exit=$?
    set -e

    if [[ $row_exit -ne 0 || -z "$row_count" ]]; then
      row_count="NULL"
    fi
  fi

  finished_iso="$(now_iso)"
  end_epoch="$(date -u +%s)"
  duration="$((end_epoch - start_epoch))"

  record_run "$step_name" "$status" "$started_iso" "$finished_iso" "$duration" "$row_count" "$error_text"

  if [[ "$status" == "failed" ]]; then
    return 1
  fi

  return 0
}

run_quality_checks() {
  local sql_file="$1"

  local started_iso
  local finished_iso
  local start_epoch
  local end_epoch
  local duration
  local status="success"
  local error_text=""
  local fail_count="0"
  local quality_output

  started_iso="$(now_iso)"
  start_epoch="$(date -u +%s)"

  echo "-> Running data quality checks (${sql_file##*/})"

  set +e
  quality_output="$(docker exec -i "$TRINO_CONTAINER" trino --output-format TSV < "$sql_file")"
  local dq_exit=$?
  set -e

  if [[ $dq_exit -ne 0 ]]; then
    status="failed"
    error_text="Data quality SQL failed for ${sql_file##*/}"
    PIPELINE_STATUS="failed"
  else
    printf '%s\n' "$quality_output"
    fail_count="$(printf '%s\n' "$quality_output" | grep -c $'\tFAIL\t' || true)"
    if [[ "$fail_count" != "0" ]]; then
      status="failed"
      error_text="${fail_count} data quality checks failed"
      PIPELINE_STATUS="failed"
    fi
  fi

  finished_iso="$(now_iso)"
  end_epoch="$(date -u +%s)"
  duration="$((end_epoch - start_epoch))"

  record_run "data_quality_checks" "$status" "$started_iso" "$finished_iso" "$duration" "$fail_count" "$error_text"

  if [[ "$status" == "failed" ]]; then
    return 1
  fi

  return 0
}

main() {
  local pipeline_started_iso
  local pipeline_finished_iso
  local pipeline_start_epoch
  local pipeline_end_epoch
  local pipeline_duration
  local pipeline_error=""

  local ops_sql="$QUERIES_DIR/20_ops_pipeline_runs.sql"
  local quality_sql="$QUERIES_DIR/19_data_quality_checks.sql"

  echo "== TF2 refresh pipeline =="
  echo "Mode: ${MODE}"
  echo "Run ID: ${RUN_ID}"
  echo "Trino container: ${TRINO_CONTAINER}"

  run_sql_file "$ops_sql"

  pipeline_started_iso="$(now_iso)"
  pipeline_start_epoch="$(date -u +%s)"

  if [[ "$MODE" == "full" ]]; then
    if ! run_step_file "build_features_player_match" "$QUERIES_DIR/11_build_features_player_match.sql" "SELECT COUNT(*) FROM tf2.default.features_player_match"; then
      pipeline_error="build_features_player_match failed"
    fi
    if [[ "$PIPELINE_STATUS" == "success" ]] && ! run_step_file "build_features_player_recent_form" "$QUERIES_DIR/12_build_features_player_recent_form.sql" "SELECT COUNT(*) FROM tf2.default.features_player_recent_form"; then
      pipeline_error="build_features_player_recent_form failed"
    fi
    if [[ "$PIPELINE_STATUS" == "success" ]] && ! run_step_file "build_serving_player_profiles" "$QUERIES_DIR/13_build_serving_player_profiles.sql" "SELECT COUNT(*) FROM tf2.default.serving_player_profiles"; then
      pipeline_error="build_serving_player_profiles failed"
    fi
    if [[ "$PIPELINE_STATUS" == "success" ]] && ! run_step_file "build_serving_map_overview_daily" "$QUERIES_DIR/14_build_serving_map_overview_daily.sql" "SELECT COUNT(*) FROM tf2.default.serving_map_overview_daily"; then
      pipeline_error="build_serving_map_overview_daily failed"
    fi
  else
    if ! run_step_file "incremental_features_player_match" "$QUERIES_DIR/15_incremental_refresh_features_player_match.sql" "SELECT COUNT(*) FROM tf2.default.features_player_match"; then
      pipeline_error="incremental_features_player_match failed"
    fi
    if [[ "$PIPELINE_STATUS" == "success" ]] && ! run_step_file "incremental_features_player_recent_form" "$QUERIES_DIR/16_incremental_refresh_features_player_recent_form.sql" "SELECT COUNT(*) FROM tf2.default.features_player_recent_form"; then
      pipeline_error="incremental_features_player_recent_form failed"
    fi
    if [[ "$PIPELINE_STATUS" == "success" ]] && ! run_step_file "incremental_serving_player_profiles" "$QUERIES_DIR/17_incremental_refresh_serving_player_profiles.sql" "SELECT COUNT(*) FROM tf2.default.serving_player_profiles"; then
      pipeline_error="incremental_serving_player_profiles failed"
    fi
    if [[ "$PIPELINE_STATUS" == "success" ]] && ! run_step_file "incremental_serving_map_overview_daily" "$QUERIES_DIR/18_incremental_refresh_serving_map_overview_daily.sql" "SELECT COUNT(*) FROM tf2.default.serving_map_overview_daily"; then
      pipeline_error="incremental_serving_map_overview_daily failed"
    fi
  fi

  if [[ "$PIPELINE_STATUS" == "success" ]] && ! run_quality_checks "$quality_sql"; then
    pipeline_error="data_quality_checks failed"
  fi

  pipeline_finished_iso="$(now_iso)"
  pipeline_end_epoch="$(date -u +%s)"
  pipeline_duration="$((pipeline_end_epoch - pipeline_start_epoch))"

  record_run "pipeline" "$PIPELINE_STATUS" "$pipeline_started_iso" "$pipeline_finished_iso" "$pipeline_duration" "NULL" "$pipeline_error"

  if [[ "$PIPELINE_STATUS" != "success" ]]; then
    echo "Pipeline failed. Check tf2.default.ops_pipeline_runs for details."
    exit 1
  fi

  echo "Pipeline completed successfully."
}

main "$@"

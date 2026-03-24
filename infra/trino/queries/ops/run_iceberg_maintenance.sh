#!/usr/bin/env bash

set -euo pipefail

TRINO_CONTAINER="${TRINO_CONTAINER:-tf2-trino}"
TRINO_CATALOG="${TRINO_CATALOG:-tf2}"
TRINO_SCHEMA="${TRINO_SCHEMA:-default}"
SNAPSHOT_RETENTION="${SNAPSHOT_RETENTION:-14d}"
OPTIMIZE_FILE_SIZE_THRESHOLD="${OPTIMIZE_FILE_SIZE_THRESHOLD:-256MB}"

TABLES=(
  logs
  summaries
  messages
  ops_pipeline_runs
  features_player_match
  features_player_recent_form
  serving_player_profiles
  serving_map_overview_daily
  serving_player_match_deep_dive
  ml_training_dataset_snapshots
  ml_training_player_match
  ml_model_registry
  ml_model_stage_history
  serving_ml_model_registry
  serving_ml_pipeline_progress_daily
)

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required" >&2
  exit 1
fi

if ! docker ps --format '{{.Names}}' | grep -q "^${TRINO_CONTAINER}$"; then
  echo "Trino container '${TRINO_CONTAINER}' is not running." >&2
  echo "Start it with: docker compose -f infra/trino/docker-compose.yml up -d" >&2
  exit 1
fi

run_trino_scalar() {
  local query="$1"
  local output
  output="$(docker exec -i "$TRINO_CONTAINER" trino --output-format TSV --execute "$query")"
  printf '%s\n' "$output" | tail -n1
}

run_trino_stmt() {
  local query="$1"
  docker exec -i "$TRINO_CONTAINER" trino --execute "$query" >/dev/null
}

table_exists() {
  local table_name="$1"
  local query="
SELECT COUNT(*)
FROM ${TRINO_CATALOG}.information_schema.tables
WHERE table_schema = '${TRINO_SCHEMA}'
  AND table_name = '${table_name}'
"

  local count
  count="$(run_trino_scalar "$query")"
  [[ "$count" != "0" ]]
}

echo "== TF2 Iceberg maintenance =="
echo "Trino container: ${TRINO_CONTAINER}"
echo "Catalog/schema: ${TRINO_CATALOG}.${TRINO_SCHEMA}"
echo "Snapshot retention: ${SNAPSHOT_RETENTION}"
echo "Optimize file threshold: ${OPTIMIZE_FILE_SIZE_THRESHOLD}"
echo

for table_name in "${TABLES[@]}"; do
  fqtn="${TRINO_CATALOG}.${TRINO_SCHEMA}.${table_name}"

  if ! table_exists "$table_name"; then
    echo "Skipping ${fqtn} (table not found)"
    continue
  fi

  echo "Maintaining ${fqtn}"

  run_trino_stmt "ALTER TABLE ${fqtn} EXECUTE optimize(file_size_threshold => '${OPTIMIZE_FILE_SIZE_THRESHOLD}')"
  run_trino_stmt "ALTER TABLE ${fqtn} EXECUTE expire_snapshots(retention_threshold => '${SNAPSHOT_RETENTION}')"
done

echo
echo "Iceberg maintenance completed."

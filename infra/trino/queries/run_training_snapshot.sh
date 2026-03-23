#!/usr/bin/env bash

set -euo pipefail

TRINO_CONTAINER="${TRINO_CONTAINER:-tf2-trino}"
QUERIES_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SNAPSHOT_SQL="$QUERIES_DIR/25_build_ml_training_snapshot.sql"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required" >&2
  exit 1
fi

if ! docker ps --format '{{.Names}}' | grep -q "^${TRINO_CONTAINER}$"; then
  echo "Trino container '${TRINO_CONTAINER}' is not running." >&2
  echo "Start it with: docker compose -f infra/trino/docker-compose.yml up -d" >&2
  exit 1
fi

echo "== TF2 ML training snapshot =="
echo "Trino container: ${TRINO_CONTAINER}"
echo "SQL file: ${SNAPSHOT_SQL}"

docker exec -i "$TRINO_CONTAINER" trino < "$SNAPSHOT_SQL"

echo
echo "Latest snapshot metadata:"
docker exec -i "$TRINO_CONTAINER" trino --output-format TSV --execute "
SELECT
  snapshot_id,
  snapshot_cutoff_time,
  source_match_rows,
  source_recent_form_rows,
  training_rows,
  created_at
FROM tf2.default.ml_training_dataset_snapshots
ORDER BY created_at DESC
LIMIT 1
"

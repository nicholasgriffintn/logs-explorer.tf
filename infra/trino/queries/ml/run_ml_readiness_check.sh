#!/usr/bin/env bash

set -euo pipefail

TRINO_CONTAINER="${TRINO_CONTAINER:-tf2-trino}"
QUERIES_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
READINESS_SQL="$QUERIES_DIR/ml_data_readiness_check.sql"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required" >&2
  exit 1
fi

if ! docker ps --format '{{.Names}}' | grep -q "^${TRINO_CONTAINER}$"; then
  echo "Trino container '${TRINO_CONTAINER}' is not running." >&2
  echo "Start it with: docker compose -f infra/trino/docker-compose.yml up -d" >&2
  exit 1
fi

echo "== TF2 ML data readiness check =="
echo "Trino container: ${TRINO_CONTAINER}"
echo "SQL file: ${READINESS_SQL}"
echo

output="$(docker exec -i "$TRINO_CONTAINER" trino --output-format TSV < "$READINESS_SQL")"
printf '%s\n' "$output"

fail_count="$(printf '%s\n' "$output" | grep -c $'\tFAIL\t' || true)"

if [[ "$fail_count" != "0" ]]; then
  echo
  echo "ML readiness check failed (${fail_count} FAIL rows)." >&2
  exit 1
fi

echo
echo "ML readiness check passed."

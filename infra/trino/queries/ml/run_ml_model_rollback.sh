#!/usr/bin/env bash

set -euo pipefail

TRINO_CONTAINER="${TRINO_CONTAINER:-tf2-trino}"
MODEL_NAME="${MODEL_NAME:-}"
TARGET_VERSION="${TARGET_VERSION:-}"
CHANGED_BY="${CHANGED_BY:-ml_operator}"
CHANGE_REASON="${CHANGE_REASON:-rollback to last known good model}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TRANSITION_SCRIPT="$SCRIPT_DIR/run_ml_model_stage_transition.sh"

if [[ -z "$MODEL_NAME" ]]; then
  echo "MODEL_NAME is required." >&2
  echo "Example: MODEL_NAME=win_probability_baseline $0" >&2
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

sql_escape() {
  printf "%s" "$1" | sed "s/'/''/g"
}

MODEL_NAME_SQL="$(sql_escape "$MODEL_NAME")"

if [[ -z "$TARGET_VERSION" ]]; then
  TARGET_VERSION="$(
    docker exec -i "$TRINO_CONTAINER" trino --output-format TSV --execute "
SELECT model_version
FROM tf2.default.ml_model_registry
WHERE model_name = '${MODEL_NAME_SQL}'
  AND COALESCE(is_active, FALSE) = FALSE
  AND stage IN ('staging', 'archived', 'production')
ORDER BY COALESCE(promoted_at, created_at) DESC
LIMIT 1
" | awk 'NR==2 {print $1}'
  )"
fi

if [[ -z "$TARGET_VERSION" ]]; then
  echo "No rollback target found for model '${MODEL_NAME}'." >&2
  echo "Set TARGET_VERSION explicitly if needed." >&2
  exit 1
fi

echo "== TF2 ML rollback =="
echo "Model: ${MODEL_NAME}"
echo "Target version: ${TARGET_VERSION}"
echo

MODEL_NAME="$MODEL_NAME" \
MODEL_VERSION="$TARGET_VERSION" \
TO_STAGE="production" \
CHANGED_BY="$CHANGED_BY" \
CHANGE_REASON="$CHANGE_REASON" \
TRINO_CONTAINER="$TRINO_CONTAINER" \
"$TRANSITION_SCRIPT"

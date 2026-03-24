#!/usr/bin/env bash

set -euo pipefail

TRINO_CONTAINER="${TRINO_CONTAINER:-tf2-trino}"
MODEL_NAME="${MODEL_NAME:-}"
MODEL_VERSION="${MODEL_VERSION:-}"
TO_STAGE="${TO_STAGE:-}"
CHANGED_BY="${CHANGED_BY:-ml_operator}"
CHANGE_REASON="${CHANGE_REASON:-manual stage transition}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required" >&2
  exit 1
fi

if ! docker ps --format '{{.Names}}' | grep -q "^${TRINO_CONTAINER}$"; then
  echo "Trino container '${TRINO_CONTAINER}' is not running." >&2
  echo "Start it with: docker compose -f infra/trino/docker-compose.yml up -d" >&2
  exit 1
fi

if [[ -z "$MODEL_NAME" || -z "$MODEL_VERSION" || -z "$TO_STAGE" ]]; then
  echo "MODEL_NAME, MODEL_VERSION, and TO_STAGE are required." >&2
  echo "Example: MODEL_NAME=win_probability_baseline MODEL_VERSION=v1.0.1 TO_STAGE=staging $0" >&2
  exit 1
fi

case "$TO_STAGE" in
  candidate|staging|production|archived) ;;
  *)
    echo "Invalid TO_STAGE '${TO_STAGE}'. Use candidate|staging|production|archived." >&2
    exit 1
    ;;
esac

sql_escape() {
  printf "%s" "$1" | sed "s/'/''/g"
}

MODEL_NAME_SQL="$(sql_escape "$MODEL_NAME")"
MODEL_VERSION_SQL="$(sql_escape "$MODEL_VERSION")"
TO_STAGE_SQL="$(sql_escape "$TO_STAGE")"
CHANGED_BY_SQL="$(sql_escape "$CHANGED_BY")"
CHANGE_REASON_SQL="$(sql_escape "$CHANGE_REASON")"

FROM_STAGE="$(
  docker exec -i "$TRINO_CONTAINER" trino --output-format TSV --execute "
SELECT stage
FROM tf2.default.ml_model_registry
WHERE model_name = '${MODEL_NAME_SQL}'
  AND model_version = '${MODEL_VERSION_SQL}'
ORDER BY created_at DESC
LIMIT 1
" | awk 'NR==2 {print $1}'
)"

if [[ -z "$FROM_STAGE" ]]; then
  echo "Model version not found in registry: ${MODEL_NAME}:${MODEL_VERSION}" >&2
  exit 1
fi

if [[ "$FROM_STAGE" == "$TO_STAGE" ]]; then
  echo "Model ${MODEL_NAME}:${MODEL_VERSION} already in stage '${TO_STAGE}'"
  exit 0
fi

if [[ "$TO_STAGE" == "production" ]]; then
  TRANSITION_SQL="
UPDATE tf2.default.ml_model_registry
SET stage = 'production',
    promoted_at = CURRENT_TIMESTAMP,
    is_active = TRUE
WHERE model_name = '${MODEL_NAME_SQL}'
  AND model_version = '${MODEL_VERSION_SQL}';

UPDATE tf2.default.ml_model_registry
SET is_active = FALSE
WHERE model_name = '${MODEL_NAME_SQL}'
  AND model_version <> '${MODEL_VERSION_SQL}'
  AND stage = 'production';
"
else
  TRANSITION_SQL="
UPDATE tf2.default.ml_model_registry
SET stage = '${TO_STAGE_SQL}',
    promoted_at = CASE
      WHEN '${TO_STAGE_SQL}' IN ('staging') THEN CURRENT_TIMESTAMP
      ELSE promoted_at
    END,
    is_active = FALSE
WHERE model_name = '${MODEL_NAME_SQL}'
  AND model_version = '${MODEL_VERSION_SQL}';
"
fi

HISTORY_SQL="
INSERT INTO tf2.default.ml_model_stage_history (
  model_name,
  model_version,
  from_stage,
  to_stage,
  changed_by,
  change_reason,
  changed_at
)
VALUES (
  '${MODEL_NAME_SQL}',
  '${MODEL_VERSION_SQL}',
  '$(sql_escape "$FROM_STAGE")',
  '${TO_STAGE_SQL}',
  '${CHANGED_BY_SQL}',
  '${CHANGE_REASON_SQL}',
  CURRENT_TIMESTAMP
);
"

echo "== TF2 ML stage transition =="
echo "Model: ${MODEL_NAME}:${MODEL_VERSION}"
echo "From: ${FROM_STAGE}"
echo "To: ${TO_STAGE}"
echo "Changed by: ${CHANGED_BY}"
echo

docker exec -i "$TRINO_CONTAINER" trino --execute "${TRANSITION_SQL}${HISTORY_SQL}"

echo "Stage transition complete."

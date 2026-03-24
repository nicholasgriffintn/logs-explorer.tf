#!/usr/bin/env bash

set -euo pipefail

TRINO_CONTAINER="${TRINO_CONTAINER:-tf2-trino}"
MODEL_NAME="${MODEL_NAME:-}"
MODEL_VERSION="${MODEL_VERSION:-}"

WIN_MIN_F1="${GATE_WIN_MIN_F1:-0.66}"
WIN_MAX_BRIER="${GATE_WIN_MAX_BRIER:-0.20}"
IMPACT_MAX_RMSE="${GATE_IMPACT_MAX_RMSE:-20.00}"
IMPACT_MAX_MAE="${GATE_IMPACT_MAX_MAE:-16.00}"
TILT_MIN_F1="${GATE_TILT_MIN_F1:-0.85}"
TILT_MAX_BRIER="${GATE_TILT_MAX_BRIER:-0.02}"
TILT_MIN_RECALL="${GATE_TILT_MIN_RECALL:-0.95}"
TILT_MIN_DAYS="${GATE_TILT_MIN_DAYS:-7}"
TILT_MAX_F1_STD="${GATE_TILT_MAX_F1_STD:-0.15}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required" >&2
  exit 1
fi

if ! docker ps --format '{{.Names}}' | grep -q "^${TRINO_CONTAINER}$"; then
  echo "Trino container '${TRINO_CONTAINER}' is not running." >&2
  echo "Start it with: docker compose -f infra/trino/docker-compose.yml up -d" >&2
  exit 1
fi

if [[ -z "$MODEL_NAME" || -z "$MODEL_VERSION" ]]; then
  echo "MODEL_NAME and MODEL_VERSION are required." >&2
  echo "Example: MODEL_NAME=win_probability_baseline MODEL_VERSION=v1.0.1 $0" >&2
  exit 1
fi

sql_escape() {
  printf "%s" "$1" | sed "s/'/''/g"
}

cmp_ge() {
  awk -v a="$1" -v b="$2" 'BEGIN { exit !((a + 0) >= (b + 0)) }'
}

cmp_le() {
  awk -v a="$1" -v b="$2" 'BEGIN { exit !((a + 0) <= (b + 0)) }'
}

is_missing_number() {
  local value="${1:-}"
  [[ -z "$value" || "$value" == "NULL" || "$value" == "null" || "$value" == "NaN" || "$value" == "nan" ]]
}

MODEL_NAME_SQL="$(sql_escape "$MODEL_NAME")"
MODEL_VERSION_SQL="$(sql_escape "$MODEL_VERSION")"

REGISTRY_SQL="
SELECT
  model_name,
  model_version,
  task_type,
  COALESCE(NULLIF(TRIM(snapshot_id), ''), '') AS snapshot_id,
  COALESCE(NULLIF(TRIM(training_code_version), ''), '') AS training_code_version,
  COALESCE(NULLIF(TRIM(feature_sql_version), ''), '') AS feature_sql_version,
  COALESCE(NULLIF(TRIM(artifact_uri), ''), '') AS artifact_uri,
  CAST(json_extract_scalar(json_parse(metrics_json), '$.f1') AS DOUBLE) AS f1,
  CAST(json_extract_scalar(json_parse(metrics_json), '$.recall') AS DOUBLE) AS recall,
  CAST(json_extract_scalar(json_parse(metrics_json), '$.brier') AS DOUBLE) AS brier,
  CAST(json_extract_scalar(json_parse(metrics_json), '$.rmse') AS DOUBLE) AS rmse,
  CAST(json_extract_scalar(json_parse(metrics_json), '$.mae') AS DOUBLE) AS mae
FROM tf2.default.ml_model_registry
WHERE model_name = '${MODEL_NAME_SQL}'
  AND model_version = '${MODEL_VERSION_SQL}'
ORDER BY created_at DESC
LIMIT 1
"

registry_output="$(docker exec -i "$TRINO_CONTAINER" trino --output-format TSV --execute "$REGISTRY_SQL")"
registry_line="$(printf '%s\n' "$registry_output" | awk 'NR==1 {print; exit}')"
if [[ -z "$registry_line" ]]; then
  echo "Model version not found in registry: ${MODEL_NAME}:${MODEL_VERSION}" >&2
  exit 1
fi

model_name_out="$(printf '%s' "$registry_line" | awk -F'\t' '{print $1}')"
model_version_out="$(printf '%s' "$registry_line" | awk -F'\t' '{print $2}')"
task_type="$(printf '%s' "$registry_line" | awk -F'\t' '{print $3}')"
snapshot_id="$(printf '%s' "$registry_line" | awk -F'\t' '{print $4}')"
training_code_version="$(printf '%s' "$registry_line" | awk -F'\t' '{print $5}')"
feature_sql_version="$(printf '%s' "$registry_line" | awk -F'\t' '{print $6}')"
artifact_uri="$(printf '%s' "$registry_line" | awk -F'\t' '{print $7}')"
f1="$(printf '%s' "$registry_line" | awk -F'\t' '{print $8}')"
recall="$(printf '%s' "$registry_line" | awk -F'\t' '{print $9}')"
brier="$(printf '%s' "$registry_line" | awk -F'\t' '{print $10}')"
rmse="$(printf '%s' "$registry_line" | awk -F'\t' '{print $11}')"
mae="$(printf '%s' "$registry_line" | awk -F'\t' '{print $12}')"

lineage_failures=0
for field_name in snapshot_id training_code_version feature_sql_version artifact_uri; do
  field_value="${!field_name:-}"
  if [[ -z "$field_value" ]]; then
    printf 'lineage_%s\tFAIL\t%s\trequired\n' "$field_name" "missing"
    lineage_failures=$((lineage_failures + 1))
  else
    printf 'lineage_%s\tPASS\t%s\trequired\n' "$field_name" "$field_value"
  fi
done

gate_failures=0
check_gate() {
  local gate_name="$1"
  local actual="$2"
  local comparator="$3"
  local target="$4"

  if is_missing_number "$actual"; then
    printf '%s\tFAIL\t%s\t%s %s\n' "$gate_name" "missing" "$comparator" "$target"
    gate_failures=$((gate_failures + 1))
    return
  fi

  local pass=1
  if [[ "$comparator" == ">=" ]]; then
    cmp_ge "$actual" "$target" || pass=0
  else
    cmp_le "$actual" "$target" || pass=0
  fi

  if [[ $pass -eq 1 ]]; then
    printf '%s\tPASS\t%s\t%s %s\n' "$gate_name" "$actual" "$comparator" "$target"
  else
    printf '%s\tFAIL\t%s\t%s %s\n' "$gate_name" "$actual" "$comparator" "$target"
    gate_failures=$((gate_failures + 1))
  fi
}

echo "== TF2 ML promotion gate check =="
echo "Model: ${MODEL_NAME}:${MODEL_VERSION}"
echo "Task: ${task_type}"
echo
printf 'gate\tstatus\tactual\ttarget\n'

case "$MODEL_NAME" in
  win_probability_baseline)
    check_gate "win_f1" "$f1" ">=" "$WIN_MIN_F1"
    check_gate "win_brier" "$brier" "<=" "$WIN_MAX_BRIER"
    ;;
  impact_percentile_baseline)
    check_gate "impact_rmse" "$rmse" "<=" "$IMPACT_MAX_RMSE"
    check_gate "impact_mae" "$mae" "<=" "$IMPACT_MAX_MAE"
    ;;
  tilt_risk_baseline)
    check_gate "tilt_f1" "$f1" ">=" "$TILT_MIN_F1"
    check_gate "tilt_brier" "$brier" "<=" "$TILT_MAX_BRIER"
    check_gate "tilt_recall" "$recall" ">=" "$TILT_MIN_RECALL"

    STABILITY_SQL="
SELECT
  COUNT(DISTINCT progress_date) AS days,
  COALESCE(STDDEV_SAMP(f1), 0.0) AS f1_stddev
FROM tf2.default.ml_model_validation_metrics_daily
WHERE model_name = '${MODEL_NAME_SQL}'
  AND model_version = '${MODEL_VERSION_SQL}'
  AND f1 IS NOT NULL
"
    stability_output="$(docker exec -i "$TRINO_CONTAINER" trino --output-format TSV --execute "$STABILITY_SQL")"
    stability_line="$(printf '%s\n' "$stability_output" | awk 'NR==1 {print; exit}')"
    if [[ -n "$stability_line" ]]; then
      IFS=$'\t' read -r tilt_days tilt_f1_std <<< "$stability_line"
      check_gate "tilt_daily_days" "$tilt_days" ">=" "$TILT_MIN_DAYS"
      check_gate "tilt_daily_f1_stddev" "$tilt_f1_std" "<=" "$TILT_MAX_F1_STD"
    else
      printf 'tilt_daily_days\tFAIL\t%s\t>= %s\n' "missing" "$TILT_MIN_DAYS"
      printf 'tilt_daily_f1_stddev\tFAIL\t%s\t<= %s\n' "missing" "$TILT_MAX_F1_STD"
      gate_failures=$((gate_failures + 2))
    fi
    ;;
  *)
    printf 'unknown_model_gate\tFAIL\t%s\t%s\n' "$MODEL_NAME" "unsupported model_name"
    gate_failures=$((gate_failures + 1))
    ;;
esac

total_failures=$((lineage_failures + gate_failures))
if [[ $total_failures -gt 0 ]]; then
  echo
  echo "Promotion gates failed (${total_failures} failed checks)." >&2
  exit 1
fi

echo
echo "Promotion gates passed."

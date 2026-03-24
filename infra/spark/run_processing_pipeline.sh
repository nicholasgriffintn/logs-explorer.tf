#!/usr/bin/env bash

set -euo pipefail

MODE="${1:-incremental}"
PIPELINE="${PIPELINE:-${2:-all}}"
REFRESH_DAYS="${REFRESH_DAYS:-7}"
SPARK_IMAGE="${SPARK_IMAGE:-logs-explorer-spark-processing:latest}"
SPARK_NETWORK="${SPARK_NETWORK:-logs-explorer}"
SPARK_ENV_FILE="${SPARK_ENV_FILE:-}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DEFAULT_SPARK_ENV_FILE="$ROOT_DIR/infra/spark/spark.env"
DOCKERFILE="$ROOT_DIR/infra/spark/Dockerfile"
JOB_SCRIPT="infra/spark/jobs/build_processing.py"

usage() {
  cat <<USAGE
Usage: $(basename "$0") [incremental|full] [all|feature-serving|ml]

Builds Spark processing tables:
  - all: features_*, serving_*, ml_training_*, ml_model_*, ops_pipeline_runs
  - feature-serving: features_*, serving_*, ops_pipeline_runs
  - ml: ml_training_*, ml_model_*, serving_ml_*, ops_pipeline_runs

Environment variables:
  PIPELINE        Pipeline slice override (all|feature-serving|ml)
  REFRESH_DAYS    Rolling refresh window in days for incremental mode (default: 7)
  SPARK_MASTER    Spark master string (default: local[4])
  SPARK_DRIVER_MEMORY  Driver heap (default: 6g)
  SPARK_EXECUTOR_MEMORY Executor heap (default: 6g)
  SPARK_SQL_SHUFFLE_PARTITIONS Shuffle partitions (default: 512)
  SPARK_DEFAULT_PARALLELISM    Default parallelism (default: 256)
  SPARK_ICEBERG_VECTORIZATION_ENABLED Iceberg vectorization toggle (default: false)
  SPARK_PARQUET_VECTORIZED_READER_ENABLED Parquet vectorized reader toggle (default: false)
  SPARK_PARQUET_NESTED_VECTORIZED_READER_ENABLED Nested Parquet vectorized reader toggle (default: false)
  SPARK_IMAGE     Spark processing image tag (default: logs-explorer-spark-processing:latest)
  SPARK_NETWORK   Docker network for job execution (default: logs-explorer)
  SPARK_ENV_FILE  Optional env file containing Spark catalog/storage settings

Required env values (direct or via SPARK_ENV_FILE):
  CATALOG_URI
  WAREHOUSE
  R2_CATALOG_TOKEN
  R2_ENDPOINT
  R2_ACCESS_KEY_ID
  R2_SECRET_ACCESS_KEY
USAGE
}

if [[ "$MODE" != "incremental" && "$MODE" != "full" ]]; then
  usage
  exit 1
fi

if [[ "$PIPELINE" != "all" && "$PIPELINE" != "feature-serving" && "$PIPELINE" != "ml" ]]; then
  usage
  exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required" >&2
  exit 1
fi

if [[ -z "$SPARK_ENV_FILE" && -f "$DEFAULT_SPARK_ENV_FILE" ]]; then
  SPARK_ENV_FILE="$DEFAULT_SPARK_ENV_FILE"
fi

if [[ -n "$SPARK_ENV_FILE" ]]; then
  if [[ ! -f "$SPARK_ENV_FILE" ]]; then
    echo "SPARK_ENV_FILE does not exist: $SPARK_ENV_FILE" >&2
    exit 1
  fi
  echo "Loading Spark configuration from: $SPARK_ENV_FILE"
  set -a
  # shellcheck disable=SC1090
  source "$SPARK_ENV_FILE"
  set +a
fi

missing=()
[[ -z "${CATALOG_URI:-}" ]] && missing+=("CATALOG_URI")
[[ -z "${WAREHOUSE:-}" ]] && missing+=("WAREHOUSE")
[[ -z "${R2_CATALOG_TOKEN:-}" ]] && missing+=("R2_CATALOG_TOKEN")
[[ -z "${R2_ENDPOINT:-}" ]] && missing+=("R2_ENDPOINT")
[[ -z "${R2_ACCESS_KEY_ID:-}" ]] && missing+=("R2_ACCESS_KEY_ID")
[[ -z "${R2_SECRET_ACCESS_KEY:-}" ]] && missing+=("R2_SECRET_ACCESS_KEY")

if (( ${#missing[@]} > 0 )); then
  echo "Missing required Spark configuration:" >&2
  for item in "${missing[@]}"; do
    echo "  - ${item}" >&2
  done
  exit 1
fi

echo "== TF2 Spark processing pipeline =="
echo "Mode: ${MODE}"
echo "Pipeline: ${PIPELINE}"
echo "Refresh days: ${REFRESH_DAYS}"
echo "Spark image: ${SPARK_IMAGE}"
echo "Spark network: ${SPARK_NETWORK}"
echo "Spark master: ${SPARK_MASTER:-local[4]}"
echo "Spark driver memory: ${SPARK_DRIVER_MEMORY:-6g}"
echo "Spark executor memory: ${SPARK_EXECUTOR_MEMORY:-6g}"
echo "Spark shuffle partitions: ${SPARK_SQL_SHUFFLE_PARTITIONS:-512}"
echo "Iceberg vectorization: ${SPARK_ICEBERG_VECTORIZATION_ENABLED:-false}"
echo "Parquet vectorized reader: ${SPARK_PARQUET_VECTORIZED_READER_ENABLED:-false}"
echo "Parquet nested vectorized reader: ${SPARK_PARQUET_NESTED_VECTORIZED_READER_ENABLED:-false}"
echo

docker build -t "$SPARK_IMAGE" -f "$DOCKERFILE" "$ROOT_DIR"

docker run --rm \
  --network "$SPARK_NETWORK" \
  -v "$ROOT_DIR:/workspace" \
  -w /workspace \
  -e MODE="$MODE" \
  -e PIPELINE="$PIPELINE" \
  -e REFRESH_DAYS="$REFRESH_DAYS" \
  -e SPARK_MASTER="${SPARK_MASTER:-local[4]}" \
  -e SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-6g}" \
  -e SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-6g}" \
  -e SPARK_SQL_SHUFFLE_PARTITIONS="${SPARK_SQL_SHUFFLE_PARTITIONS:-512}" \
  -e SPARK_DEFAULT_PARALLELISM="${SPARK_DEFAULT_PARALLELISM:-256}" \
  -e SPARK_ICEBERG_VECTORIZATION_ENABLED="${SPARK_ICEBERG_VECTORIZATION_ENABLED:-false}" \
  -e SPARK_PARQUET_VECTORIZED_READER_ENABLED="${SPARK_PARQUET_VECTORIZED_READER_ENABLED:-false}" \
  -e SPARK_PARQUET_NESTED_VECTORIZED_READER_ENABLED="${SPARK_PARQUET_NESTED_VECTORIZED_READER_ENABLED:-false}" \
  -e JOB_SCRIPT="$JOB_SCRIPT" \
  -e CATALOG_URI="$CATALOG_URI" \
  -e WAREHOUSE="$WAREHOUSE" \
  -e R2_CATALOG_TOKEN="$R2_CATALOG_TOKEN" \
  -e R2_ENDPOINT="$R2_ENDPOINT" \
  -e R2_ACCESS_KEY_ID="$R2_ACCESS_KEY_ID" \
  -e R2_SECRET_ACCESS_KEY="$R2_SECRET_ACCESS_KEY" \
  "$SPARK_IMAGE"

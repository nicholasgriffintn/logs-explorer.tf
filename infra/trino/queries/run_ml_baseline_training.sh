#!/usr/bin/env bash

set -euo pipefail

TRINO_CONTAINER="${TRINO_CONTAINER:-tf2-trino}"
TRINO_HOST="${TRINO_HOST:-tf2-trino}"
TRINO_PORT="${TRINO_PORT:-8080}"
TRINO_USER="${TRINO_USER:-ml_trainer}"
TRINO_CATALOG="${TRINO_CATALOG:-tf2}"
TRINO_SCHEMA="${TRINO_SCHEMA:-default}"
TRINO_HTTP_SCHEME="${TRINO_HTTP_SCHEME:-http}"
MODEL_VERSION="${MODEL_VERSION:-v1.0.0}"
TRAIN_RATIO="${TRAIN_RATIO:-0.8}"
SNAPSHOT_ID="${SNAPSHOT_ID:-}"
IMAGE_NAME="${ML_TRAINER_IMAGE:-logs-explorer-ml-trainer:latest}"
NETWORK_NAME="${ML_TRAINER_NETWORK:-logs-explorer}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
DOCKERFILE="$ROOT_DIR/infra/ml/Dockerfile"
TRAINING_CODE_VERSION="${TRAINING_CODE_VERSION:-$(git -C "$ROOT_DIR" rev-parse HEAD 2>/dev/null || echo unknown)}"
FEATURE_SQL_VERSION="${FEATURE_SQL_VERSION:-$TRAINING_CODE_VERSION}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required" >&2
  exit 1
fi

if ! docker ps --format '{{.Names}}' | grep -q "^${TRINO_CONTAINER}$"; then
  echo "Trino container '${TRINO_CONTAINER}' is not running." >&2
  echo "Start it with: docker compose -f infra/trino/docker-compose.yml up -d" >&2
  exit 1
fi

echo "== TF2 ML baseline training =="
echo "Trino container: ${TRINO_CONTAINER}"
echo "Trino endpoint: ${TRINO_HTTP_SCHEME}://${TRINO_HOST}:${TRINO_PORT}"
echo "Model version: ${MODEL_VERSION}"
echo "Image: ${IMAGE_NAME}"
echo "Network: ${NETWORK_NAME}"
echo

docker build -t "$IMAGE_NAME" -f "$DOCKERFILE" "$ROOT_DIR"

docker run --rm \
  --network "$NETWORK_NAME" \
  -v "$ROOT_DIR:/workspace" \
  -w /workspace \
  -e TRINO_HOST="$TRINO_HOST" \
  -e TRINO_PORT="$TRINO_PORT" \
  -e TRINO_USER="$TRINO_USER" \
  -e TRINO_CATALOG="$TRINO_CATALOG" \
  -e TRINO_SCHEMA="$TRINO_SCHEMA" \
  -e TRINO_HTTP_SCHEME="$TRINO_HTTP_SCHEME" \
  -e MODEL_VERSION="$MODEL_VERSION" \
  -e TRAIN_RATIO="$TRAIN_RATIO" \
  -e SNAPSHOT_ID="$SNAPSHOT_ID" \
  -e TRAINING_CODE_VERSION="$TRAINING_CODE_VERSION" \
  -e FEATURE_SQL_VERSION="$FEATURE_SQL_VERSION" \
  "$IMAGE_NAME"

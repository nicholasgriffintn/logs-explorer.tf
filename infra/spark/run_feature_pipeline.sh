#!/usr/bin/env bash

set -euo pipefail

MODE="${1:-incremental}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

exec "$SCRIPT_DIR/run_processing_pipeline.sh" "$MODE" feature-serving

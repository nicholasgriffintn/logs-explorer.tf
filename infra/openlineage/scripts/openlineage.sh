#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/infra/openlineage/docker-compose.yml"
OPENLINEAGE_ENV_FILE="$ROOT_DIR/infra/openlineage/openlineage.env"
OPENLINEAGE_ENV_EXAMPLE="$ROOT_DIR/infra/openlineage/openlineage.env.example"

usage() {
  cat <<USAGE
Usage: infra/openlineage/scripts/openlineage.sh <command>

Commands:
  up        Start Marquez API, DB, and web UI
  down      Stop the OpenLineage stack
  restart   Restart the OpenLineage stack
  status    Show service status
  logs      Follow service logs
USAGE
}

compose() {
  if [[ -f "$OPENLINEAGE_ENV_FILE" ]]; then
    docker compose --env-file "$OPENLINEAGE_ENV_FILE" -f "$COMPOSE_FILE" "$@"
  else
    docker compose -f "$COMPOSE_FILE" "$@"
  fi
}

ensure_openlineage_env() {
  if [[ -f "$OPENLINEAGE_ENV_FILE" ]]; then
    return
  fi

  cp "$OPENLINEAGE_ENV_EXAMPLE" "$OPENLINEAGE_ENV_FILE"
  echo "Created $OPENLINEAGE_ENV_FILE from example. Review image tags/ports before first run."
}

ensure_shared_network() {
  if docker network inspect logs-explorer >/dev/null 2>&1; then
    return
  fi

  docker network create logs-explorer >/dev/null
}

command_name="${1:-}"
if [[ -z "$command_name" ]]; then
  usage
  exit 1
fi

shift || true

case "$command_name" in
  up)
    ensure_openlineage_env
    ensure_shared_network
    compose up -d postgres marquez marquez-web
    ;;
  down)
    compose down
    ;;
  restart)
    compose restart postgres marquez marquez-web
    ;;
  status)
    compose ps
    ;;
  logs)
    compose logs -f --tail 200 postgres marquez marquez-web
    ;;
  *)
    usage
    exit 1
    ;;
esac

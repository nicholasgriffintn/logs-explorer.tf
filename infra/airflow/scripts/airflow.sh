#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/infra/airflow/docker-compose.yml"
AIRFLOW_ENV_FILE="$ROOT_DIR/infra/airflow/airflow.env"
AIRFLOW_ENV_EXAMPLE="$ROOT_DIR/infra/airflow/airflow.env.example"
SPARK_ENV_FILE="$ROOT_DIR/infra/spark/spark.env"
API_SERVER_CONTAINER="tf2-airflow-api-server"

usage() {
  cat <<USAGE
Usage: infra/airflow/scripts/airflow.sh <command> [args]

Commands:
  up                                Build and start the full Airflow stack
  down                              Stop the Airflow stack
  restart                           Restart Airflow runtime services
  status                            Show status for Airflow services
  logs                              Follow Airflow runtime logs
  dags                              List DAGs
  unpause <dag_id>                  Unpause a DAG
  pause <dag_id>                    Pause a DAG
  trigger <dag_id> [json_conf]      Trigger a DAG run
  trigger-e2e [json_conf]           Trigger tf2_platform_e2e_daily

Examples:
  infra/airflow/scripts/airflow.sh up
  infra/airflow/scripts/airflow.sh dags
  infra/airflow/scripts/airflow.sh trigger tf2_feature_serving_daily
  infra/airflow/scripts/airflow.sh trigger-e2e '{"run_baseline_training": true, "model_version": "v1.2.0"}'
USAGE
}

compose() {
  docker compose -f "$COMPOSE_FILE" "$@"
}

ensure_airflow_env() {
  if [[ -f "$AIRFLOW_ENV_FILE" ]]; then
    return
  fi

  cp "$AIRFLOW_ENV_EXAMPLE" "$AIRFLOW_ENV_FILE"
  echo "Created $AIRFLOW_ENV_FILE from example. Review credentials and schedule settings before first run."
}

ensure_spark_env() {
  if [[ -f "$SPARK_ENV_FILE" ]]; then
    return
  fi

  echo "Missing Spark configuration: $SPARK_ENV_FILE" >&2
  echo "Create it from infra/spark/spark.env.example and set real catalog/storage credentials." >&2
  exit 1
}

ensure_shared_network() {
  if docker network inspect logs-explorer >/dev/null 2>&1; then
    return
  fi

  docker network create logs-explorer >/dev/null
}

ensure_running() {
  if ! docker ps --format '{{.Names}}' | grep -q "^${API_SERVER_CONTAINER}$"; then
    echo "Airflow API server container '${API_SERVER_CONTAINER}' is not running." >&2
    echo "Start it with: infra/airflow/scripts/airflow.sh up" >&2
    exit 1
  fi
}

airflow_cli() {
  ensure_running
  docker exec -i "$API_SERVER_CONTAINER" airflow "$@"
}

command_name="${1:-}"
if [[ -z "$command_name" ]]; then
  usage
  exit 1
fi

shift || true

case "$command_name" in
  up)
    ensure_airflow_env
    ensure_spark_env
    ensure_shared_network
    compose up airflow-init --build
    compose up -d --build airflow-api-server airflow-scheduler airflow-dag-processor airflow-worker airflow-triggerer
    ;;
  down)
    compose down
    ;;
  restart)
    compose restart airflow-api-server airflow-scheduler airflow-dag-processor airflow-worker airflow-triggerer
    ;;
  status)
    compose ps
    ;;
  logs)
    compose logs -f --tail 200 airflow-api-server airflow-scheduler airflow-dag-processor airflow-worker airflow-triggerer
    ;;
  dags)
    airflow_cli dags list
    ;;
  unpause)
    dag_id="${1:-}"
    if [[ -z "$dag_id" ]]; then
      echo "dag_id is required" >&2
      exit 1
    fi
    airflow_cli dags unpause "$dag_id"
    ;;
  pause)
    dag_id="${1:-}"
    if [[ -z "$dag_id" ]]; then
      echo "dag_id is required" >&2
      exit 1
    fi
    airflow_cli dags pause "$dag_id"
    ;;
  trigger)
    dag_id="${1:-}"
    conf="${2:-}"
    if [[ -z "$dag_id" ]]; then
      echo "dag_id is required" >&2
      exit 1
    fi

    if [[ -n "$conf" ]]; then
      airflow_cli dags trigger "$dag_id" --conf "$conf"
    else
      airflow_cli dags trigger "$dag_id"
    fi
    ;;
  trigger-e2e)
    conf="${1:-}"
    if [[ -n "$conf" ]]; then
      airflow_cli dags trigger tf2_platform_e2e_daily --conf "$conf"
    else
      airflow_cli dags trigger tf2_platform_e2e_daily
    fi
    ;;
  *)
    usage
    exit 1
    ;;
esac

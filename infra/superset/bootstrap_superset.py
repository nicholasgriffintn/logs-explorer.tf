#!/usr/bin/env python3
"""Idempotent Superset bootstrap for TF2 dashboards."""

import json
import http.cookiejar
import os
import socket
import time
import urllib.error
import urllib.parse
import urllib.request

from sqlalchemy import create_engine
from sqlalchemy.exc import NoSuchModuleError


BASE_URL = os.environ.get("SUPERSET_BASE_URL", "http://tf2-superset:8088").rstrip("/")
ADMIN_USERNAME = os.environ.get("SUPERSET_ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.environ.get("SUPERSET_ADMIN_PASSWORD", "admin")
TRINO_URI = os.environ.get(
  "SUPERSET_TRINO_SQLALCHEMY_URI",
  "trino://trino@tf2-trino:8080/tf2/default",
)
QUERY_DIR = os.environ.get("SUPERSET_QUERY_DIR", "/workspace/trino-queries")

DATABASE_NAME = "TF2 Trino"
DATASETS = (
  "serving_player_profiles",
  "serving_map_overview_daily",
  "serving_player_match_deep_dive",
  "serving_ml_model_registry",
  "serving_ml_pipeline_progress_daily",
)

DASHBOARDS = (
  "TF2 Player Profile and Momentum",
  "TF2 Map Competitiveness and Pace",
  "TF2 Chat Behaviour and Tilt Risk",
  "TF2 Player Match Deep Dive",
  "TF2 ML Progress and Registry",
)

SAVED_QUERY_FILES = (
  ("Player Profile and Momentum", "21_dashboard_player_profile_and_momentum.sql"),
  ("Map Competitiveness and Pace", "22_dashboard_map_competitiveness_and_pace.sql"),
  ("Chat Behaviour and Tilt Risk", "23_dashboard_chat_behaviour_and_tilt_risk.sql"),
  ("Player Match Deep Dive", "31_dashboard_player_match_deep_dive.sql"),
  ("ML Progress and Registry", "32_dashboard_ml_progress_and_registry.sql"),
)

DASHBOARD_CHART_SPECS = {
  "TF2 Player Profile and Momentum": [
    {
      "slice_name": "Player Momentum Snapshot",
      "dataset": "serving_player_profiles",
      "layout_width": 12,
      "layout_height": 56,
      "variants": [
        {
          "viz_type": "table",
          "form_data": {
            "query_mode": "raw",
            "all_columns": [
              "steamid",
              "games_played",
              "career_win_rate",
              "career_avg_impact",
              "rolling_10_avg_impact",
              "form_delta_impact",
              "momentum_label",
              "tilt_risk_rate",
              "rolling_10_negative_chat_ratio",
              "updated_at",
            ],
            "row_limit": 100,
          },
        }
      ],
    },
    {
      "slice_name": "Momentum Cohort Distribution",
      "dataset": "serving_player_profiles",
      "layout_width": 6,
      "layout_height": 50,
      "variants": [
        {
          "viz_type": "pie",
          "form_data": {
            "query_mode": "aggregate",
            "groupby": ["momentum_label"],
            "metric": {
              "expressionType": "SQL",
              "sqlExpression": "COUNT(*)",
              "label": "Players",
              "hasCustomLabel": True,
            },
            "metrics": [
              {
                "expressionType": "SQL",
                "sqlExpression": "COUNT(*)",
                "label": "Players",
                "hasCustomLabel": True,
              }
            ],
            "adhoc_filters": [],
            "row_limit": 1000,
            "sort_by_metric": True,
          },
        },
        {
          "viz_type": "dist_bar",
          "form_data": {
            "query_mode": "aggregate",
            "groupby": ["momentum_label"],
            "metric": {
              "expressionType": "SQL",
              "sqlExpression": "COUNT(*)",
              "label": "Players",
              "hasCustomLabel": True,
            },
            "metrics": [
              {
                "expressionType": "SQL",
                "sqlExpression": "COUNT(*)",
                "label": "Players",
                "hasCustomLabel": True,
              }
            ],
            "adhoc_filters": [],
            "row_limit": 1000,
            "sort_by_metric": True,
          },
        },
        {
          "viz_type": "table",
          "form_data": {
            "query_mode": "raw",
            "all_columns": [
              "steamid",
              "momentum_label",
              "games_played",
              "career_avg_impact",
              "tilt_risk_rate",
            ],
            "row_limit": 200,
          },
        },
      ],
    },
    {
      "slice_name": "Impact vs Tilt Bubble",
      "dataset": "serving_player_profiles",
      "layout_width": 6,
      "layout_height": 50,
      "variants": [
        {
          "viz_type": "table",
          "form_data": {
            "query_mode": "raw",
            "all_columns": [
              "steamid",
              "momentum_label",
              "games_played",
              "career_avg_impact",
              "tilt_risk_rate",
            ],
            "row_limit": 150,
          },
        },
      ],
    },
  ],
  "TF2 Map Competitiveness and Pace": [
    {
      "slice_name": "Map Competitiveness Snapshot",
      "dataset": "serving_map_overview_daily",
      "layout_width": 12,
      "layout_height": 56,
      "variants": [
        {
          "viz_type": "table",
          "form_data": {
            "query_mode": "raw",
            "all_columns": [
              "match_date",
              "map",
              "games",
              "avg_duration_minutes",
              "avg_kills_per_minute",
              "close_game_rate",
              "blowout_rate",
              "active_players",
              "avg_negative_chat_ratio",
              "tilt_signal_rate",
            ],
            "row_limit": 200,
          },
        }
      ],
    },
    {
      "slice_name": "Daily Games Trend by Map",
      "dataset": "serving_map_overview_daily",
      "layout_width": 12,
      "layout_height": 50,
      "variants": [
        {
          "viz_type": "echarts_timeseries_line",
          "form_data": {
            "query_mode": "aggregate",
            "granularity_sqla": "match_date",
            "time_grain_sqla": "P1D",
            "time_range": "No filter",
            "since": "180 days ago",
            "until": "now",
            "groupby": ["map"],
            "metric": {
              "expressionType": "SQL",
              "sqlExpression": "SUM(games)",
              "label": "Total Games",
              "hasCustomLabel": True,
            },
            "metrics": [
              {
                "expressionType": "SQL",
                "sqlExpression": "SUM(games)",
                "label": "Total Games",
                "hasCustomLabel": True,
              }
            ],
            "adhoc_filters": [],
            "row_limit": 5000,
            "show_legend": True,
          },
        },
        {
          "viz_type": "table",
          "form_data": {
            "query_mode": "raw",
            "all_columns": ["match_date", "map", "games", "active_players"],
            "row_limit": 300,
          },
        },
      ],
    },
    {
      "slice_name": "Match Quality Trend",
      "dataset": "serving_map_overview_daily",
      "layout_width": 12,
      "layout_height": 50,
      "variants": [
        {
          "viz_type": "echarts_timeseries_line",
          "form_data": {
            "query_mode": "aggregate",
            "granularity_sqla": "match_date",
            "time_grain_sqla": "P1D",
            "time_range": "No filter",
            "since": "180 days ago",
            "until": "now",
            "groupby": [],
            "metrics": [
              {
                "expressionType": "SQL",
                "sqlExpression": "SUM(close_game_rate * games) / NULLIF(SUM(games), 0)",
                "label": "Game-Weighted Close Game Rate",
                "hasCustomLabel": True,
              },
              {
                "expressionType": "SQL",
                "sqlExpression": "SUM(blowout_rate * games) / NULLIF(SUM(games), 0)",
                "label": "Game-Weighted Blowout Rate",
                "hasCustomLabel": True,
              },
            ],
            "adhoc_filters": [],
            "row_limit": 5000,
            "show_legend": True,
          },
        },
        {
          "viz_type": "table",
          "form_data": {
            "query_mode": "raw",
            "all_columns": ["match_date", "map", "close_game_rate", "blowout_rate", "games"],
            "row_limit": 300,
          },
        },
      ],
    },
  ],
  "TF2 Chat Behaviour and Tilt Risk": [
    {
      "slice_name": "Player Tilt Risk Snapshot",
      "dataset": "serving_player_profiles",
      "layout_width": 6,
      "layout_height": 50,
      "variants": [
        {
          "viz_type": "table",
          "form_data": {
            "query_mode": "raw",
            "all_columns": [
              "steamid",
              "momentum_label",
              "rolling_10_negative_chat_ratio",
              "tilt_risk_rate",
              "recent_30_win_rate",
              "form_delta_impact",
            ],
            "row_limit": 120,
          },
        }
      ],
    },
    {
      "slice_name": "Map Chat Signal Snapshot",
      "dataset": "serving_map_overview_daily",
      "layout_width": 6,
      "layout_height": 50,
      "variants": [
        {
          "viz_type": "table",
          "form_data": {
            "query_mode": "raw",
            "all_columns": [
              "match_date",
              "map",
              "games",
              "avg_negative_chat_ratio",
              "tilt_signal_rate",
              "active_players",
            ],
            "row_limit": 120,
          },
        }
      ],
    },
    {
      "slice_name": "Map Tilt Signal Trend",
      "dataset": "serving_map_overview_daily",
      "layout_width": 12,
      "layout_height": 50,
      "variants": [
        {
          "viz_type": "echarts_timeseries_line",
          "form_data": {
            "query_mode": "aggregate",
            "granularity_sqla": "match_date",
            "time_grain_sqla": "P1D",
            "time_range": "No filter",
            "since": "90 days ago",
            "until": "now",
            "groupby": ["map"],
            "metric": {
              "expressionType": "SQL",
              "sqlExpression": "AVG(tilt_signal_rate)",
              "label": "Avg Tilt Signal Rate",
              "hasCustomLabel": True,
            },
            "metrics": [
              {
                "expressionType": "SQL",
                "sqlExpression": "AVG(tilt_signal_rate)",
                "label": "Avg Tilt Signal Rate",
                "hasCustomLabel": True,
              }
            ],
            "adhoc_filters": [],
            "row_limit": 5000,
            "show_legend": True,
          },
        },
        {
          "viz_type": "table",
          "form_data": {
            "query_mode": "raw",
            "all_columns": ["match_date", "map", "tilt_signal_rate", "games"],
            "row_limit": 300,
          },
        },
      ],
    },
    {
      "slice_name": "Chat vs Tilt Hotspots",
      "dataset": "serving_map_overview_daily",
      "layout_width": 12,
      "layout_height": 50,
      "variants": [
        {
          "viz_type": "table",
          "form_data": {
            "query_mode": "raw",
            "all_columns": [
              "match_date",
              "map",
              "games",
              "avg_negative_chat_ratio",
              "tilt_signal_rate",
            ],
            "row_limit": 300,
          },
        },
      ],
    },
  ],
  "TF2 Player Match Deep Dive": [
    {
      "slice_name": "Player Match Deep-Dive Snapshot",
      "dataset": "serving_player_match_deep_dive",
      "layout_width": 12,
      "layout_height": 56,
      "variants": [
        {
          "viz_type": "table",
          "form_data": {
            "query_mode": "raw",
            "all_columns": [
              "match_date",
              "steamid",
              "logid",
              "map",
              "won_game",
              "kills",
              "damage_dealt",
              "impact_index",
              "kda_ratio",
              "negative_chat_ratio",
              "momentum_label",
              "behaviour_risk_tier",
              "impact_tier",
            ],
            "row_limit": 250,
          },
        }
      ],
    },
    {
      "slice_name": "Player Match Impact Trend",
      "dataset": "serving_player_match_deep_dive",
      "layout_width": 12,
      "layout_height": 50,
      "variants": [
        {
          "viz_type": "echarts_timeseries_line",
          "form_data": {
            "query_mode": "aggregate",
            "granularity_sqla": "match_date",
            "time_grain_sqla": "P1D",
            "time_range": "No filter",
            "since": "120 days ago",
            "until": "now",
            "groupby": [],
            "metrics": [
              {
                "expressionType": "SQL",
                "sqlExpression": "AVG(impact_index)",
                "label": "Avg Impact Index",
                "hasCustomLabel": True,
              },
              {
                "expressionType": "SQL",
                "sqlExpression": "AVG(negative_chat_ratio)",
                "label": "Avg Negative Chat Ratio",
                "hasCustomLabel": True,
              },
            ],
            "adhoc_filters": [],
            "row_limit": 5000,
            "show_legend": True,
          },
        },
        {
          "viz_type": "table",
          "form_data": {
            "query_mode": "raw",
            "all_columns": [
              "match_date",
              "steamid",
              "map",
              "impact_index",
              "negative_chat_ratio",
              "won_game",
            ],
            "row_limit": 400,
          },
        },
      ],
    },
    {
      "slice_name": "Behaviour Risk Split",
      "dataset": "serving_player_match_deep_dive",
      "layout_width": 6,
      "layout_height": 50,
      "variants": [
        {
          "viz_type": "pie",
          "form_data": {
            "query_mode": "aggregate",
            "groupby": ["behaviour_risk_tier"],
            "metric": {
              "expressionType": "SQL",
              "sqlExpression": "COUNT(*)",
              "label": "Matches",
              "hasCustomLabel": True,
            },
            "metrics": [
              {
                "expressionType": "SQL",
                "sqlExpression": "COUNT(*)",
                "label": "Matches",
                "hasCustomLabel": True,
              }
            ],
            "adhoc_filters": [],
            "row_limit": 5000,
            "sort_by_metric": True,
          },
        },
        {
          "viz_type": "dist_bar",
          "form_data": {
            "query_mode": "aggregate",
            "groupby": ["behaviour_risk_tier"],
            "metric": {
              "expressionType": "SQL",
              "sqlExpression": "COUNT(*)",
              "label": "Matches",
              "hasCustomLabel": True,
            },
            "metrics": [
              {
                "expressionType": "SQL",
                "sqlExpression": "COUNT(*)",
                "label": "Matches",
                "hasCustomLabel": True,
              }
            ],
            "adhoc_filters": [],
            "row_limit": 5000,
            "sort_by_metric": True,
          },
        },
      ],
    },
    {
      "slice_name": "Impact Tier Distribution",
      "dataset": "serving_player_match_deep_dive",
      "layout_width": 6,
      "layout_height": 50,
      "variants": [
        {
          "viz_type": "pie",
          "form_data": {
            "query_mode": "aggregate",
            "groupby": ["impact_tier"],
            "metric": {
              "expressionType": "SQL",
              "sqlExpression": "COUNT(*)",
              "label": "Matches",
              "hasCustomLabel": True,
            },
            "metrics": [
              {
                "expressionType": "SQL",
                "sqlExpression": "COUNT(*)",
                "label": "Matches",
                "hasCustomLabel": True,
              }
            ],
            "adhoc_filters": [],
            "row_limit": 5000,
            "sort_by_metric": True,
          },
        },
        {
          "viz_type": "table",
          "form_data": {
            "query_mode": "raw",
            "all_columns": ["impact_tier", "steamid", "logid", "match_date", "impact_index"],
            "row_limit": 300,
          },
        },
      ],
    },
  ],
  "TF2 ML Progress and Registry": [
    {
      "slice_name": "ML Pipeline Progress Daily",
      "dataset": "serving_ml_pipeline_progress_daily",
      "layout_width": 12,
      "layout_height": 56,
      "variants": [
        {
          "viz_type": "table",
          "form_data": {
            "query_mode": "raw",
            "all_columns": [
              "progress_date",
              "pipeline_success_runs",
              "pipeline_failed_runs",
              "avg_pipeline_duration_seconds",
              "latest_pipeline_status",
              "snapshots_created",
              "training_rows_materialised",
              "latest_snapshot_id",
              "models_registered",
              "promotions_to_production",
              "active_production_models",
            ],
            "row_limit": 200,
          },
        }
      ],
    },
    {
      "slice_name": "ML Activity Trend",
      "dataset": "serving_ml_pipeline_progress_daily",
      "layout_width": 12,
      "layout_height": 50,
      "variants": [
        {
          "viz_type": "echarts_timeseries_line",
          "form_data": {
            "query_mode": "aggregate",
            "granularity_sqla": "progress_date",
            "time_grain_sqla": "P1D",
            "time_range": "No filter",
            "since": "180 days ago",
            "until": "now",
            "groupby": [],
            "metrics": [
              {
                "expressionType": "SQL",
                "sqlExpression": "SUM(snapshots_created)",
                "label": "Snapshots Created",
                "hasCustomLabel": True,
              },
              {
                "expressionType": "SQL",
                "sqlExpression": "SUM(models_registered)",
                "label": "Models Registered",
                "hasCustomLabel": True,
              },
              {
                "expressionType": "SQL",
                "sqlExpression": "SUM(promotions_to_production)",
                "label": "Promotions to Production",
                "hasCustomLabel": True,
              },
            ],
            "adhoc_filters": [],
            "row_limit": 5000,
            "show_legend": True,
          },
        },
        {
          "viz_type": "table",
          "form_data": {
            "query_mode": "raw",
            "all_columns": [
              "progress_date",
              "snapshots_created",
              "models_registered",
              "promotions_to_production",
              "active_production_models",
            ],
            "row_limit": 300,
          },
        },
      ],
    },
    {
      "slice_name": "Model Stage Distribution",
      "dataset": "serving_ml_model_registry",
      "layout_width": 6,
      "layout_height": 50,
      "variants": [
        {
          "viz_type": "pie",
          "form_data": {
            "query_mode": "aggregate",
            "groupby": ["stage"],
            "metric": {
              "expressionType": "SQL",
              "sqlExpression": "COUNT(*)",
              "label": "Model Versions",
              "hasCustomLabel": True,
            },
            "metrics": [
              {
                "expressionType": "SQL",
                "sqlExpression": "COUNT(*)",
                "label": "Model Versions",
                "hasCustomLabel": True,
              }
            ],
            "adhoc_filters": [],
            "row_limit": 5000,
            "sort_by_metric": True,
          },
        },
        {
          "viz_type": "dist_bar",
          "form_data": {
            "query_mode": "aggregate",
            "groupby": ["stage"],
            "metric": {
              "expressionType": "SQL",
              "sqlExpression": "COUNT(*)",
              "label": "Model Versions",
              "hasCustomLabel": True,
            },
            "metrics": [
              {
                "expressionType": "SQL",
                "sqlExpression": "COUNT(*)",
                "label": "Model Versions",
                "hasCustomLabel": True,
              }
            ],
            "adhoc_filters": [],
            "row_limit": 5000,
            "sort_by_metric": True,
          },
        },
      ],
    },
    {
      "slice_name": "ML Model Registry Versions",
      "dataset": "serving_ml_model_registry",
      "layout_width": 6,
      "layout_height": 50,
      "variants": [
        {
          "viz_type": "table",
          "form_data": {
            "query_mode": "raw",
            "all_columns": [
              "model_name",
              "model_version",
              "task_type",
              "stage",
              "is_active",
              "snapshot_id",
              "primary_metric_name",
              "primary_metric_value",
              "model_age_days",
              "data_age_days",
            ],
            "row_limit": 300,
          },
        },
      ],
    },
  ],
}

MAX_ERROR_BODY_CHARS = 1200
COOKIE_JAR = http.cookiejar.CookieJar()
HTTP_OPENER = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(COOKIE_JAR))


class ApiError(RuntimeError):
  def __init__(self, code: int, path: str, details: str):
    super().__init__(f"HTTP {code} for {path}: {details}")
    self.code = code
    self.path = path
    self.details = details


def log(message: str) -> None:
  print(f"[superset-bootstrap] {message}", flush=True)


def can_reach_sqlalchemy_uri(sqlalchemy_uri: str, timeout: float = 3.0) -> tuple[bool, str]:
  parsed = urllib.parse.urlparse(sqlalchemy_uri)
  host = parsed.hostname
  port = parsed.port or 8080
  if not host:
    return False, "missing host in sqlalchemy URI"

  try:
    with socket.create_connection((host, port), timeout=timeout):
      return True, f"reachable tcp://{host}:{port}"
  except OSError as err:
    return False, f"unreachable tcp://{host}:{port} ({err})"


def ensure_trino_dialect_available() -> None:
  try:
    create_engine(TRINO_URI)
  except NoSuchModuleError as err:
    raise RuntimeError(
      "Trino SQLAlchemy dialect is missing in the Superset image. "
      "Rebuild with `docker compose -f infra/superset/docker-compose.yml up --build -d`."
    ) from err


def request_json(
  method: str,
  path: str,
  payload: dict | None = None,
  headers: dict | None = None,
  timeout: int = 20,
) -> dict:
  url = f"{BASE_URL}{path}"
  data = None
  request_headers = {"Content-Type": "application/json"}
  if headers:
    request_headers.update(headers)
  if payload is not None:
    data = json.dumps(payload).encode("utf-8")

  req = urllib.request.Request(url=url, data=data, headers=request_headers, method=method)
  try:
    with HTTP_OPENER.open(req, timeout=timeout) as response:
      body = response.read()
      if not body:
        return {}
      return json.loads(body.decode("utf-8"))
  except urllib.error.HTTPError as err:
    details = err.read().decode("utf-8", errors="replace").strip()
    if len(details) > MAX_ERROR_BODY_CHARS:
      details = details[:MAX_ERROR_BODY_CHARS] + "...(truncated)"
    raise ApiError(code=err.code, path=path, details=details) from err


def request_json_or_none(
  method: str,
  path: str,
  payload: dict | None = None,
  headers: dict | None = None,
) -> dict | None:
  try:
    return request_json(method=method, path=path, payload=payload, headers=headers)
  except ApiError as err:
    if err.code in (400, 404, 409, 422):
      log(f"Skipping {path}: HTTP {err.code} {err.details}")
      return None
    raise


def get_result_list(response: dict) -> list:
  result = response.get("result", [])
  if isinstance(result, list):
    return result
  return []


def request_raw(method: str, path: str, headers: dict | None = None, timeout: int = 20) -> None:
  url = f"{BASE_URL}{path}"
  req = urllib.request.Request(url=url, headers=headers or {}, method=method)
  with HTTP_OPENER.open(req, timeout=timeout):
    return


def wait_for_superset(max_attempts: int = 30, sleep_seconds: int = 2) -> None:
  last_error = None
  for attempt in range(1, max_attempts + 1):
    try:
      request_raw("GET", "/health")
      log(f"Superset reachable on attempt {attempt}")
      return
    except Exception as err:  # noqa: BLE001
      last_error = err
      time.sleep(sleep_seconds)

  raise RuntimeError(
    f"Superset is not reachable at {BASE_URL}. "
    "Start Superset with `docker compose -f infra/superset/docker-compose.yml up -d` "
    f"and retry bootstrap. Last error: {last_error}"
  )


def prime_cookie_session() -> None:
  wait_for_superset()
  # Superset binds CSRF tokens to the Flask session cookie.
  # We prime the cookie jar with a GET to /login/ before API auth.
  request_raw("GET", "/login/")
  cookie_names = sorted({cookie.name for cookie in COOKIE_JAR})
  log(f"Primed cookie session. Cookies present: {cookie_names}")


def login_with_retry(max_attempts: int = 30, sleep_seconds: int = 3) -> tuple[str, str]:
  payload = {
    "username": ADMIN_USERNAME,
    "password": ADMIN_PASSWORD,
    "provider": "db",
    "refresh": True,
  }

  last_error = None
  for attempt in range(1, max_attempts + 1):
    try:
      login_response = request_json("POST", "/api/v1/security/login", payload=payload)
      access_token = login_response["access_token"]
      auth_headers = {"Authorization": f"Bearer {access_token}"}
      csrf_response = request_json("GET", "/api/v1/security/csrf_token/", headers=auth_headers)
      csrf_token = csrf_response["result"]
      cookie_names = sorted({cookie.name for cookie in COOKIE_JAR})
      log(f"Session cookies available after auth: {cookie_names}")
      log(f"Authenticated on attempt {attempt}")
      return access_token, csrf_token
    except Exception as err:  # noqa: BLE001
      last_error = err
      log(f"Auth attempt {attempt}/{max_attempts} failed, retrying in {sleep_seconds}s")
      time.sleep(sleep_seconds)

  raise RuntimeError(f"Failed to authenticate to Superset: {last_error}")


def auth_headers(access_token: str, csrf_token: str) -> dict:
  return {
    "Authorization": f"Bearer {access_token}",
    "X-CSRFToken": csrf_token,
    "Referer": BASE_URL,
  }


def find_existing_database(headers: dict) -> dict | None:
  response = request_json("GET", "/api/v1/database/?page_size=500", headers=headers)
  for item in get_result_list(response):
    if item.get("database_name") == DATABASE_NAME:
      return item
  return None


def create_database_with_uri(headers: dict, sqlalchemy_uri: str) -> int:
  payload_candidates = (
    {
      "database_name": DATABASE_NAME,
      "sqlalchemy_uri": sqlalchemy_uri,
      "configuration_method": "sqlalchemy_form",
    },
    {
      "database_name": DATABASE_NAME,
      "sqlalchemy_uri": sqlalchemy_uri,
    },
    {
      "database_name": DATABASE_NAME,
      "sqlalchemy_uri": sqlalchemy_uri,
      "configuration_method": "sqlalchemy_form",
      "expose_in_sqllab": True,
      "allow_ctas": False,
      "allow_cvas": False,
      "allow_dml": False,
    },
  )

  errors: list[str] = []
  for index, payload in enumerate(payload_candidates, start=1):
    try:
      created = request_json("POST", "/api/v1/database/", payload=payload, headers=headers)
      db_id = int(created["id"])
      log(f"Created database: {DATABASE_NAME} (id={db_id}, payload_variant={index})")
      return db_id
    except ApiError as err:
      err_summary = (
        f"uri={sqlalchemy_uri} payload_variant={index} status={err.code} details={err.details}"
      )
      errors.append(err_summary)
      log(f"Database create attempt failed: {err_summary}")

      existing_after_error = find_existing_database(headers)
      if existing_after_error:
        db_id = int(existing_after_error["id"])
        log(f"Database found after failed create attempt: {DATABASE_NAME} (id={db_id})")
        return db_id

  joined_errors = " | ".join(errors)
  raise RuntimeError(joined_errors)


def ensure_database(headers: dict) -> int:
  existing = find_existing_database(headers)
  if existing:
    db_id = int(existing["id"])
    log(f"Database already exists: {DATABASE_NAME} (id={db_id})")
    return db_id

  reachable, reachability_note = can_reach_sqlalchemy_uri(TRINO_URI)
  log(f"Testing configured Trino URI {TRINO_URI}: {reachability_note}")
  if not reachable:
    raise RuntimeError(
      "Unable to create or find database "
      f"'{DATABASE_NAME}'. Trino is not reachable at {TRINO_URI}. "
      "Start Trino with `docker compose -f infra/trino/docker-compose.yml up -d` "
      "and rerun bootstrap."
    )

  try:
    return create_database_with_uri(headers, TRINO_URI)
  except RuntimeError as err:
    raise RuntimeError(
      "Unable to create or find database "
      f"'{DATABASE_NAME}' using configured URI {TRINO_URI}. "
      "Ensure Trino is running and the URI is valid. "
      f"Diagnostics: {err}"
    ) from err


def list_datasets(headers: dict) -> list:
  response = request_json("GET", "/api/v1/dataset/?page_size=2000", headers=headers)
  return get_result_list(response)


def ensure_datasets(headers: dict, database_id: int) -> dict[str, int]:
  existing = list_datasets(headers)
  existing_index = {
    (item.get("database", {}).get("id"), item.get("schema"), item.get("table_name")): item
    for item in existing
  }
  for table_name in DATASETS:
    key = (database_id, "default", table_name)
    if key in existing_index:
      log(f"Dataset already exists: {table_name}")
      continue

    payload = {
      "database": database_id,
      "schema": "default",
      "table_name": table_name,
    }
    created = request_json("POST", "/api/v1/dataset/", payload=payload, headers=headers)
    log(f"Created dataset: {table_name} (id={created.get('id')})")

  refreshed = list_datasets(headers)
  dataset_ids: dict[str, int] = {}
  for item in refreshed:
    if (
      item.get("database", {}).get("id") == database_id
      and item.get("schema") == "default"
      and item.get("table_name") in DATASETS
    ):
      dataset_ids[item["table_name"]] = int(item["id"])

  missing = [table for table in DATASETS if table not in dataset_ids]
  if missing:
    raise RuntimeError(f"Missing datasets after bootstrap: {missing}")

  return dataset_ids


def list_dashboards(headers: dict) -> list:
  response = request_json("GET", "/api/v1/dashboard/?page_size=500", headers=headers)
  return get_result_list(response)


def ensure_dashboards(headers: dict) -> dict[str, int]:
  existing_titles = {item.get("dashboard_title") for item in list_dashboards(headers)}

  for title in DASHBOARDS:
    if title in existing_titles:
      log(f"Dashboard already exists: {title}")
      continue
    payload = {"dashboard_title": title, "published": True}
    created = request_json_or_none("POST", "/api/v1/dashboard/", payload=payload, headers=headers)
    if created is not None:
      log(f"Created dashboard: {title}")

  refreshed = list_dashboards(headers)
  dashboard_ids = {
    item["dashboard_title"]: int(item["id"])
    for item in refreshed
    if item.get("dashboard_title") in DASHBOARDS
  }
  missing = [title for title in DASHBOARDS if title not in dashboard_ids]
  if missing:
    raise RuntimeError(f"Missing dashboards after bootstrap: {missing}")
  return dashboard_ids


def list_charts(headers: dict) -> list:
  response = request_json("GET", "/api/v1/chart/?page_size=2000", headers=headers)
  return get_result_list(response)


def coerce_layout_value(value: object, default: int, minimum: int, maximum: int) -> int:
  try:
    parsed = int(value)
  except (TypeError, ValueError):
    return default
  return max(minimum, min(maximum, parsed))


def build_chart_payload(
  dataset_id: int,
  dashboard_id: int,
  slice_name: str,
  variant: dict,
) -> tuple[str, dict]:
  viz_type = str(variant["viz_type"])
  form_data = {
    "datasource": f"{dataset_id}__table",
    "viz_type": viz_type,
  }
  variant_form_data = variant.get("form_data", {})
  if isinstance(variant_form_data, dict):
    form_data.update(variant_form_data)
  if "adhoc_filters" not in form_data:
    form_data["adhoc_filters"] = []

  payload = {
    "slice_name": slice_name,
    "viz_type": viz_type,
    "datasource_id": dataset_id,
    "datasource_type": "table",
    "params": json.dumps(form_data, sort_keys=True),
    "dashboards": [dashboard_id],
  }
  return viz_type, payload


def upsert_chart(
  headers: dict,
  dashboard_id: int,
  dataset_id: int,
  spec: dict,
  existing_chart: dict | None,
) -> int:
  slice_name = str(spec["slice_name"])
  variants = spec.get("variants", [])
  if not isinstance(variants, list) or not variants:
    raise RuntimeError(f"Missing chart variants for {slice_name}")

  existing_chart_id = int(existing_chart["id"]) if existing_chart else None
  errors: list[str] = []
  for variant in variants:
    if not isinstance(variant, dict):
      continue
    viz_type, payload = build_chart_payload(
      dataset_id=dataset_id,
      dashboard_id=dashboard_id,
      slice_name=slice_name,
      variant=variant,
    )
    try:
      if existing_chart_id is not None:
        request_json("PUT", f"/api/v1/chart/{existing_chart_id}", payload=payload, headers=headers)
        log(f"Updated chart: {slice_name} (id={existing_chart_id}, viz_type={viz_type})")
        return existing_chart_id

      created = request_json("POST", "/api/v1/chart/", payload=payload, headers=headers)
      chart_id = int(created["id"])
      log(f"Created chart: {slice_name} (id={chart_id}, viz_type={viz_type})")
      return chart_id
    except ApiError as err:
      errors.append(f"{viz_type}: HTTP {err.code} {err.details}")
      log(
        f"Chart variant failed for {slice_name} "
        f"(viz_type={viz_type}, status={err.code}); trying fallback"
      )

  joined_errors = " | ".join(errors)
  raise RuntimeError(f"Unable to create/update chart {slice_name}: {joined_errors}")


def ensure_charts_and_layouts(
  headers: dict,
  dashboard_ids: dict[str, int],
  dataset_ids: dict[str, int],
) -> None:
  charts_by_name = {item.get("slice_name"): item for item in list_charts(headers)}

  for dashboard_title, specs in DASHBOARD_CHART_SPECS.items():
    dashboard_id = dashboard_ids[dashboard_title]
    layout_chart_nodes: list[tuple[int, str, int, int]] = []

    for spec in specs:
      dataset_name = str(spec["dataset"])
      dataset_id = dataset_ids[dataset_name]
      slice_name = str(spec["slice_name"])

      chart_id = upsert_chart(
        headers=headers,
        dashboard_id=dashboard_id,
        dataset_id=dataset_id,
        spec=spec,
        existing_chart=charts_by_name.get(slice_name),
      )
      charts_by_name[slice_name] = {"id": chart_id, "slice_name": slice_name}

      width = coerce_layout_value(spec.get("layout_width", 12), default=12, minimum=1, maximum=12)
      height = coerce_layout_value(spec.get("layout_height", 50), default=50, minimum=20, maximum=90)
      layout_chart_nodes.append((chart_id, slice_name, width, height))

    root = {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"]}
    grid_children: list[str] = []
    row_children: dict[str, list[str]] = {}
    layout: dict[str, object] = {
      "DASHBOARD_VERSION_KEY": "v2",
      "ROOT_ID": root,
      "GRID_ID": {"id": "GRID_ID", "type": "GRID", "parents": ["ROOT_ID"], "children": grid_children},
    }

    current_row_id: str | None = None
    current_row_width = 0
    row_number = 0

    for index, (chart_id, slice_name, width, height) in enumerate(layout_chart_nodes, start=1):
      if current_row_id is None or current_row_width + width > 12:
        row_number += 1
        current_row_id = f"ROW-{row_number}"
        current_row_width = 0
        grid_children.append(current_row_id)
        row_children[current_row_id] = []
        layout[current_row_id] = {
          "id": current_row_id,
          "type": "ROW",
          "parents": ["ROOT_ID", "GRID_ID"],
          "children": row_children[current_row_id],
          "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }

      chart_node_id = f"CHART-{chart_id}-{index}"
      row_children[current_row_id].append(chart_node_id)
      layout[chart_node_id] = {
        "id": chart_node_id,
        "type": "CHART",
        "parents": ["ROOT_ID", "GRID_ID", current_row_id],
        "children": [],
        "meta": {
          "chartId": chart_id,
          "height": height,
          "width": width,
          "sliceName": slice_name,
        },
      }
      current_row_width += width
      if current_row_width >= 12:
        current_row_id = None

    payload = {"position_json": json.dumps(layout, sort_keys=True)}
    request_json("PUT", f"/api/v1/dashboard/{dashboard_id}", payload=payload, headers=headers)
    log(f"Updated dashboard layout with charts: {dashboard_title}")


def load_sql_file(filename: str) -> str | None:
  path = os.path.join(QUERY_DIR, filename)
  if not os.path.exists(path):
    log(f"Query file not found, skipping: {path}")
    return None
  with open(path, "r", encoding="utf-8") as handle:
    return handle.read().strip()


def ensure_saved_queries(headers: dict, database_id: int) -> None:
  existing_resp = request_json_or_none("GET", "/api/v1/saved_query/?page_size=500", headers=headers)
  if existing_resp is None:
    return

  existing = get_result_list(existing_resp)
  existing_labels = {item.get("label") for item in existing}

  for label, filename in SAVED_QUERY_FILES:
    if label in existing_labels:
      log(f"Saved query already exists: {label}")
      continue

    sql = load_sql_file(filename)
    if not sql:
      continue

    payload = {
      "label": label,
      "db_id": database_id,
      "schema": "default",
      "sql": sql,
    }
    created = request_json_or_none("POST", "/api/v1/saved_query/", payload=payload, headers=headers)
    if created is not None:
      log(f"Created saved query: {label}")


def main() -> int:
  ensure_trino_dialect_available()
  prime_cookie_session()
  access_token, csrf_token = login_with_retry()
  headers = auth_headers(access_token=access_token, csrf_token=csrf_token)

  database_id = ensure_database(headers)
  dataset_ids = ensure_datasets(headers, database_id)
  dashboard_ids = ensure_dashboards(headers)
  ensure_charts_and_layouts(headers, dashboard_ids, dataset_ids)
  ensure_saved_queries(headers, database_id)

  log("Bootstrap completed")
  return 0


if __name__ == "__main__":
  raise SystemExit(main())

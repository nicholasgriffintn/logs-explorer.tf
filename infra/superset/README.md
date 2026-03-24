# Apache Superset setup

This directory provides a local Superset instance for building dashboards from `serving_*` tables.

## Start Superset

Start Trino first and keep it running:

```bash
docker compose -f infra/trino/docker-compose.yml up -d
```

This is part of the main platform flow in `docs/data-platform-e2e-workflow.md`.

Run Spark feature-serving pipeline before bootstrap:

```bash
infra/spark/run_feature_pipeline.sh incremental
```

If ML progress dashboards must be current, run ML refresh separately:

```bash
infra/spark/run_ml_pipeline.sh incremental
```

Then start Superset:

```bash
docker compose -f infra/superset/docker-compose.yml up --build -d
```

Open `http://localhost:8088` and log in with:

- username: `admin`
- password: `admin`

Change credentials via environment variables in your shell before startup:

- `SUPERSET_ADMIN_USERNAME`
- `SUPERSET_ADMIN_PASSWORD`
- `SUPERSET_ADMIN_EMAIL`
- `SUPERSET_SECRET_KEY`
- `SUPERSET_TRINO_SQLALCHEMY_URI` (optional)

## Automatic bootstrap

Bootstrap creates a Superset database named `TF2 Trino` using:

```text
trino://trino@tf2-trino:8080/tf2/default
```

Bootstrap also creates datasets:

- `serving_player_profiles`
- `serving_map_overview_daily`
- `serving_player_match_deep_dive`
- `serving_ml_model_registry`
- `serving_ml_pipeline_progress_daily`

And creates:

- dashboard shell: player profile and momentum
- dashboard shell: map competitiveness and pace
- dashboard shell: chat behaviour and tilt risk
- dashboard shell: player match deep dive
- dashboard shell: ML progress and registry
- saved query source: `infra/trino/queries/21_dashboard_player_profile_and_momentum.sql`
- saved query source: `infra/trino/queries/22_dashboard_map_competitiveness_and_pace.sql`
- saved query source: `infra/trino/queries/23_dashboard_chat_behaviour_and_tilt_risk.sql`
- saved query source: `infra/trino/queries/31_dashboard_player_match_deep_dive.sql`
- saved query source: `infra/trino/queries/32_dashboard_ml_progress_and_registry.sql`

To run bootstrap again manually:

```bash
docker compose -f infra/superset/docker-compose.yml run --rm superset-bootstrap
```

## Stop Superset

```bash
docker compose -f infra/superset/docker-compose.yml down
```

To also remove Superset metadata storage:

```bash
docker compose -f infra/superset/docker-compose.yml down -v
```

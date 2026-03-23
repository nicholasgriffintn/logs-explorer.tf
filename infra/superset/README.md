# Apache Superset setup

This directory provides a local Superset instance for building dashboards from `serving_*` tables.

## Start Superset

Start Trino first:

```bash
docker compose -f infra/trino/docker-compose.yml up -d
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

And creates:

- dashboard shell: player profile and momentum
- dashboard shell: map competitiveness and pace
- dashboard shell: chat behaviour and tilt risk
- saved query source: `infra/trino/queries/21_dashboard_player_profile_and_momentum.sql`
- saved query source: `infra/trino/queries/22_dashboard_map_competitiveness_and_pace.sql`
- saved query source: `infra/trino/queries/23_dashboard_chat_behaviour_and_tilt_risk.sql`

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

# Apache Superset setup

This directory provides a local Superset instance for building dashboards from `serving_*` tables.

## Start Superset

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

## Connect Superset to Trino

When adding a database in Superset, use:

```text
trino://trino@host.docker.internal:8080/tf2/default
```

Then create datasets from:

- `serving_player_profiles`
- `serving_map_overview_daily`

## Stop Superset

```bash
docker compose -f infra/superset/docker-compose.yml down
```

To also remove Superset metadata storage:

```bash
docker compose -f infra/superset/docker-compose.yml down -v
```

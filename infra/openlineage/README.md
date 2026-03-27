# OpenLineage runtime

This stack provides a local OpenLineage backend using Marquez.

## Services

- `postgres`: metadata database for Marquez
- `marquez`: OpenLineage API (`http://localhost:5000`)
- `marquez-web`: lineage UI (`http://localhost:3000`)

## Quick start

```bash
cp infra/openlineage/openlineage.env.example infra/openlineage/openlineage.env
infra/openlineage/scripts/openlineage.sh up
```

## Operations

```bash
infra/openlineage/scripts/openlineage.sh status
infra/openlineage/scripts/openlineage.sh logs
infra/openlineage/scripts/openlineage.sh down
```

## Integration

- Airflow uses `http://marquez:5000/api/v1/lineage` inside the `logs-explorer` Docker network.
- Ingest worker can post directly to `http://localhost:5000/api/v1/lineage` when running locally.

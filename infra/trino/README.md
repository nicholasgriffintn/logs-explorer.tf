# Apache Trino setup

This directory provides a local Trino runtime configured for Cloudflare R2 Data Catalog.
Trino is used for query serving, quality/readiness SQL execution, and dashboard reads.

## Configure catalog

```bash
cp infra/trino/catalog/tf2.properties.example infra/trino/catalog/tf2.properties
```

Fill placeholders using values from your Cloudflare R2 Data Catalog and R2 API token.

## Start Trino

```bash
docker compose -f infra/trino/docker-compose.yml up -d
```

## Connect

```bash
docker exec -it tf2-trino trino
```

Example query:

```sql
SHOW SCHEMAS FROM tf2;
SHOW TABLES FROM tf2.default;
SELECT logid, map, sourcedateiso FROM tf2.default.logs ORDER BY logid DESC LIMIT 20;
```

## Airflow integration

Airflow uses Trino through the configured Trino connection (`trino_default` by default) for:

- serving quality checks
- ML readiness checks
- Iceberg maintenance statements

For orchestration commands, use `infra/airflow/README.md`.

# Apache Trino setup

This directory provides a local Trino runtime configured for Cloudflare R2 Data Catalog.

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
SHOW TABLES FROM tf2.tf2_core;
SHOW TABLES FROM tf2.tf2_chat;
SHOW TABLES FROM tf2.tf2_players;
SELECT logid, map, sourcedateiso FROM tf2.tf2_core.logs ORDER BY logid DESC LIMIT 20;
```

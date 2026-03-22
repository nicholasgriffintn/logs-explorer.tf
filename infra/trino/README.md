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
# Verify connection and schema
SHOW SCHEMAS FROM tf2;

# List tables in the default schema
SHOW TABLES FROM tf2.default;

# Sample query to preview logs data
SELECT logid, map, sourcedateiso FROM tf2.default.logs ORDER BY logid DESC LIMIT 20;

# Sample query to preview chat data
SELECT logid, map, sourcedateiso FROM tf2.default.messages ORDER BY logid DESC LIMIT 20;

# Sample query to preview summaries data
SELECT logid, map, sourcedateiso FROM tf2.default.summaries ORDER BY logid DESC LIMIT 20;
```

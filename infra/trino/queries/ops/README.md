# Trino ops queries

Operational scripts for Iceberg table health and maintenance.

## Iceberg maintenance

Run:

```bash
infra/trino/queries/ops/run_iceberg_maintenance.sh
```

This script:

- detects which target tables currently exist
- compacts files with Iceberg `optimize`
- expires snapshots older than `SNAPSHOT_RETENTION`

Environment overrides:

- `TRINO_CONTAINER` (default `tf2-trino`)
- `TRINO_CATALOG` (default `tf2`)
- `TRINO_SCHEMA` (default `default`)
- `SNAPSHOT_RETENTION` (default `14d`)
- `OPTIMIZE_FILE_SIZE_THRESHOLD` (default `256MB`)

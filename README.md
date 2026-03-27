# TF2 Logs Explorer

Platform for ingesting and exploring public Team Fortress 2 logs from `logs.tf` using Cloudflare Pipelines, R2 Data Catalog, Apache Spark, Apache Airflow, Apache Trino, and OpenLineage.

> NOTE: This was built with the help of AI, I did quite a bit of work but the AI was also super helpful. I'm sharing this as an example of how these sorts of systems can be built with Cloudflare services and also as a platform to have a bit of fun with myself.

## Monorepo layout

- `apps/ingest-service`: scheduled Cloudflare Worker that polls `logs.tf`, fetches details, and emits records into Pipelines.
- `packages/tf2-log-model`: shared runtime validation + normalisation for logs list and detail payloads.
- `infra/cloudflare/pipelines`: stream schema and setup instructions for Cloudflare Pipelines.
- `infra/airflow`: orchestration runtime, DAGs, and operational command wrappers.
- `infra/openlineage`: local OpenLineage backend runtime (Marquez + web UI).
- `infra/trino`: local Trino stack and catalog config template for querying R2 Data Catalog.
- `infra/spark`: Spark processing pipelines for `features_*`, `serving_*`, and ML table materialisation.
- `infra/aws`: AWS CDK stack for EMR Trino, ECS Superset, ECS Marquez/OpenLineage, and MWAA.

## Setup

### 1. Install dependencies

```bash
pnpm install
```

### 2. Configure Pipelines and R2 Data Catalog

Follow:

- `infra/cloudflare/pipelines/README.md`

### 3. Configure the ingest service

```bash
cp apps/ingest-service/.dev.vars.example apps/ingest-service/.dev.vars
```

Then set:

- Worker bindings:
  - `INGEST_CURSOR_KV`
  - `TF2_LOGS_STREAM`
  - `TF2_CHAT_STREAM`
  - `TF2_PLAYERS_STREAM`

Update `apps/ingest-service/wrangler.jsonc` with your KV namespace ID and stream IDs.
The checked-in file currently includes all required bindings; replace IDs with your own values before deployment.

### 4. Configure Spark and Trino

Follow:

- `infra/spark/README.md`
- `infra/trino/README.md`

### 5. Configure and start OpenLineage

Follow:

- `infra/openlineage/README.md`

### 6. Configure and start Airflow orchestration

Follow:

- `infra/airflow/README.md`

### 7. Run ingest service locally

```bash
pnpm --filter @logs-explorer/ingest-service dev
```

### 8. (Optional) Deploy AWS-managed platform components

See:

- `infra/aws/README.md`

## Analytics and dashboards

- End-to-end Airflow-first run flow: `docs/data-platform-e2e-workflow.md`
- Airflow runtime and DAG operations: `infra/airflow/README.md`
- Spark processing pipelines: `infra/spark/README.md`
- Query index and analytics SQL pack: `infra/trino/queries/README.md`
- Superset dashboard workspace setup: `infra/superset/README.md`
- Refresh operations and recovery guidance: `docs/refresh-operations-runbook.md`
- Serving performance benchmark/tuning notes: `docs/serving-query-performance-tuning.md`
- Platform expansion plans (Atlas, Ranger, Pinot/Druid): `docs/platform-expansion/README.md`

## Machine learning operations

- ML pipeline run and snapshot workflow: `docs/ml-training-snapshot-workflow.md`
- Model registry policy: `docs/model-registry-and-versioning-policy.md`
- Stage transition tooling: `infra/trino/queries/ml/run_ml_model_stage_transition.sh`
- Rollback tooling: `infra/trino/queries/ml/run_ml_model_rollback.sh`

## Development commands

- `pnpm dev`: run ingest service locally
- `pnpm openlineage:up`: start Marquez/OpenLineage stack
- `pnpm openlineage:status`: show OpenLineage service status
- `pnpm openlineage:down`: stop OpenLineage stack
- `pnpm airflow:up`: start full Airflow stack
- `pnpm airflow:status`: show Airflow service status
- `pnpm airflow:dags`: list registered DAGs
- `pnpm airflow:trigger:e2e`: trigger the full E2E DAG
- `pnpm airflow:down`: stop Airflow stack
- `pnpm aws:cdk:synth`: synthesise AWS CDK platform template
- `pnpm aws:cdk:deploy`: deploy AWS CDK platform stack
- `pnpm test`: run tests across the monorepo
- `pnpm build`: run build scripts across the monorepo
- `pnpm check`: format, lint, test, build

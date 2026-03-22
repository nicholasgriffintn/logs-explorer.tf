# TF2 Logs Explorer

Platform for ingesting and exploring public Team Fortress 2 logs from `logs.tf` using Cloudflare Pipelines, R2 Data Catalog, and Apache Trino.

> NOTE: This was built with the help of AI, I did quite a bit of work but the AI was also super helpful. I'm sharing this as an example of how these sorts of systems can be built with Cloudflare services and also as a platform to have a bit of fun with myself.

## Monorepo layout

- `apps/ingest-service`: scheduled Cloudflare Worker that polls `logs.tf`, fetches details, and emits records into Pipelines.
- `packages/tf2-log-model`: shared runtime validation + normalisation for logs list and detail payloads.
- `infra/cloudflare/pipelines`: stream schema and setup instructions for Cloudflare Pipelines.
- `infra/trino`: local Trino stack and catalog config template for querying R2 Data Catalog.

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

### 4. Run ingest service locally

```bash
pnpm --filter @logs-explorer/ingest-service dev
```

### 5. Configure Trino

Follow:

- `infra/trino/README.md`

## Analytics and dashboards

You can find the query index, starter queries, and run commands in: `infra/trino/queries/README.md`

## Development commands

- `pnpm dev`: run ingest service locally
- `pnpm test`: run tests across the monorepo
- `pnpm build`: run build scripts across the monorepo
- `pnpm check`: format, lint, test, build

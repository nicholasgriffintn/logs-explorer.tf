# Ingest service

Cloudflare Worker that incrementally ingests TF2 logs from `logs.tf` into Cloudflare Pipelines.

## Behaviour

- polls `https://logs.tf/api/v1/log`
- fetches full details from `https://logs.tf/api/v1/log/:id`
- validates and normalises payloads using `@logs-explorer/tf2-log-model`
- writes cursor and failure retry state to `INGEST_CURSOR_KV`
- emits fan-out datasets:
  - core logs to `TF2_LOGS_STREAM`
  - chat messages to `TF2_CHAT_STREAM`
  - player summaries to `TF2_PLAYERS_STREAM`

## Endpoints

- `GET /health`
- `GET /ingest?dryRun=true`
- `POST /ingest`

## Local dev

```bash
cp .dev.vars.example .dev.vars
pnpm --filter @logs-explorer/ingest-service dev
```

You can then test the endpoints with:

```bash
curl http://localhost:8787/health
curl -X POST http://localhost:8787/ingest?dryRun=true
```

## Required bindings

`apps/ingest-service/wrangler.jsonc` must include:

- `INGEST_CURSOR_KV`
- `TF2_LOGS_STREAM` (required)
- `TF2_CHAT_STREAM` (required)
- `TF2_PLAYERS_STREAM` (required)

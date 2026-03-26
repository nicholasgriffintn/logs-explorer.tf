# Ingest service

Cloudflare Worker that incrementally ingests TF2 logs from `logs.tf` into Cloudflare Pipelines.

## Source log structure

### `/api/v1/log` list payload

The list endpoint returns summaries like:

```json
{
  "success": true,
  "results": 1000,
  "total": 4010879,
  "parameters": {},
  "logs": [
    {
      "id": 4031052,
      "title": "serveme.tf #1537392 BLU vs RED",
      "map": "cp_snakewater_final1",
      "date": 1774213787,
      "views": 0,
      "players": 13
    }
  ]
}
```

### `/api/v1/log/:id` detail payload

Detail payloads include nested match data (`teams`, `players`, `rounds`, `healspread`, `chat`, etc).
The ingest service fans this into dedicated analytics datasets (core logs, chat messages, player summaries).

## Ingestion design

The ingest service is designed to avoid overloading `logs.tf` and to prevent silent data loss:

- incremental cursor (`lastIngestedLogId`) stored in Worker KV
- bounded pagination per run (`LOGS_TF_MAX_PAGES_PER_RUN`)
- optional full-history mode with queue-chained pagination (`INGEST_BACKFILL_QUEUE`)
- request spacing (`LOGS_TF_REQUEST_DELAY_MS`) and retry budget (`LOGS_TF_FETCH_RETRIES`)
- retry queue for failed logs with exponential backoff and per-run retry cap (`LOGS_TF_MAX_RETRY_LOGS_PER_RUN`)
- failed logs are suppressed from "new" candidate selection until retry backoff is due
- retry progress tracks dataset delivery (`logs`, `chat`, `players`) so partial failures resume without re-emitting already delivered records
- per-log downstream emission with record-capped sends (`PIPELINES_BATCH_SIZE`)
- strict payload validation before emission

This gives predictable API pressure, resumable ingestion, and safer long-running operation.

## Data contracts to Pipelines

### Core logs table (`logs`, typically `tf2.default.logs`)

From stream: `TF2_LOGS_STREAM`

- `recordId` (idempotency key)
- `logId`, `title`, `map`
- source metadata (`sourceDateEpochSeconds`, `sourceDateIso`, `sourcePlayerCount`, `sourceViewCount`)
- match summary (`durationSeconds`, `redScore`, `blueScore`)
- provenance (`uploaderSteamId`, `payloadSchemaVersion`, `ingestedAt`)

Schema: `infra/cloudflare/pipelines/tf2-log-stream.schema.json`

### Chat messages table (`messages`, typically `tf2.default.messages`)

From stream: `TF2_CHAT_STREAM`

- one row per chat line with `message`, `messageLower`, `steamId`, `playerName`
- enables moderation workflows (for example slur keyword filters) and chat behaviour analysis

Schema: `infra/cloudflare/pipelines/tf2-chat-stream.schema.json`

### Player summaries table (`summaries`, typically `tf2.default.summaries`)

From stream: `TF2_PLAYERS_STREAM`

- one row per player per log with kills/assists/deaths/damage/healing/ubers/team/classes
- supports per-class, per-player, and per-map performance analytics

Schema: `infra/cloudflare/pipelines/tf2-player-stream.schema.json`

## Endpoints

- `GET /health`
- `GET /ingest?dryRun=true`
- `POST /ingest`
- `POST /ingest?mode=full-history[&offset=0]` (run one full-history chunk immediately)
- `POST /ingest/full-history/start[?offset=0]` (seed queue-driven full-history run)

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
- `INGEST_BACKFILL_QUEUE` (required for queue-driven full-history mode)

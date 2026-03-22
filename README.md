# TF2 Logs Explorer

Platform for ingesting and exploring public Team Fortress 2 logs from `logs.tf` using Cloudflare Pipelines, R2 Data Catalog, and Apache Trino.

> NOTE: This was built with AI as a quick example of how this data could be ingested and explored. It is not under active development, I'm just having a bit of fun.

## Monorepo layout

- `apps/ingest-service`: scheduled Cloudflare Worker that polls `logs.tf`, fetches details, and emits records into Pipelines.
- `packages/tf2-log-model`: shared runtime validation + normalisation for logs list and detail payloads.
- `infra/cloudflare/pipelines`: stream schema and SQL transform for Cloudflare Pipelines.
- `infra/trino`: local Trino stack and catalog config template for querying R2 Data Catalog.

## Source log structure

### `/api/v1/logs` list payload

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

## Ingestion design (responsible mode)

The ingest service is designed to avoid overloading `logs.tf` and to prevent silent data loss:

- incremental cursor (`lastIngestedLogId`) stored in Worker KV
- bounded pagination per run (`LOGS_TF_MAX_PAGES_PER_RUN`)
- request spacing (`LOGS_TF_REQUEST_DELAY_MS`) and retry budget (`LOGS_TF_FETCH_RETRIES`)
- retry queue for failed logs with exponential backoff
- batched downstream writes (`PIPELINES_BATCH_SIZE`)
- strict payload validation before emission

This gives predictable API pressure, resumable ingestion, and safer long-running operation.

## Data contracts to Pipelines

### Core logs dataset (`tf2_core.logs`)

- `recordId` (idempotency key)
- `logId`, `title`, `map`
- source metadata (`sourceDateEpochSeconds`, `sourceDateIso`, `sourcePlayerCount`, `sourceViewCount`)
- match summary (`durationSeconds`, `redScore`, `blueScore`)
- provenance (`uploaderSteamId`, `payloadSchemaVersion`, `ingestedAt`)

Schema: `infra/cloudflare/pipelines/tf2-log-stream.schema.json`

### Chat messages dataset (`tf2_chat.messages`)

- one row per chat line with `message`, `messageLower`, `steamId`, `playerName`
- enables moderation workflows (for example slur keyword filters) and chat behaviour analysis

Schema: `infra/cloudflare/pipelines/tf2-chat-stream.schema.json`

### Player summaries dataset (`tf2_players.summaries`)

- one row per player per log with kills/assists/deaths/damage/healing/ubers/team/classes
- supports per-class, per-player, and per-map performance analytics

Schema: `infra/cloudflare/pipelines/tf2-player-stream.schema.json`

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
  - `TF2_LOGS_STREAM` (required)
  - `TF2_CHAT_STREAM` (required)
  - `TF2_PLAYERS_STREAM` (required)

Update `apps/ingest-service/wrangler.jsonc` with your KV namespace ID and stream IDs.
The file now has deploy-safe defaults (required logs binding + optional bindings commented out).

### 4. Run ingest service locally

```bash
pnpm --filter @logs-explorer/ingest-service dev
```

### 5. Configure Trino

Follow:

- `infra/trino/README.md`

## Development commands

- `pnpm dev`: run ingest service locally
- `pnpm test`: run tests across the monorepo
- `pnpm build`: run build scripts across the monorepo
- `pnpm ready`: format, lint, test, build

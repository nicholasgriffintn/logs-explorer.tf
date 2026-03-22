# Cloudflare Pipelines setup

This folder defines a fan-out ingest model for TF2 analytics datasets.

## Dataset layout

- `tf2_logs_stream` -> `tf2_core.logs` (one row per log)
- `tf2_chat_stream` -> `tf2_chat.messages` (one row per chat message)
- `tf2_players_stream` -> `tf2_players.summaries` (one row per player per log)

This keeps storage and query costs predictable while supporting moderation and gameplay analytics.

## 1. CREATE R2 bucket and catalog

```bash
npx wrangler r2 bucket create tf2-data-lake
npx wrangler r2 bucket catalog enable tf2-data-lake
```

## 2. Generate API token

Pipelines must authenticate to R2 Data Catalog with an R2 API token that has catalog and R2 permissions.

In the Cloudflare dashboard, go to the R2 object storage page.

1. Go to -> Overview -> Select Manage API tokens.
2. Select Create Account API token.
3. Give your API token a name.
4. Under Permissions, select the Admin Read & Write permission.
5. Select Create Account API Token.
6. Note the Token value.

## 2. Create Logs Pipeline

Run the following command:

`npx wrangler pipelines setup`

Follow the prompts:

Pipeline name: tf2_logs

Stream configuration:

Enable HTTP endpoint: no
Require authentication: no (for simplicity)
Configure custom CORS origins: no
Schema definition: Load from file
Schema file path: ./infra/cloudflare/pipelines/tf2-log-stream.schema.json

Sink configuration:

Destination type: Data Catalog (Iceberg)
Setup mode: Simple (recommended defaults)
R2 bucket name: tf2-data-lake
Table name: logs
Catalog API token: (provide your token)
Review: Confirm the summary and select Create resources

SQL transformation: Choose Simple ingestion (SELECT \* FROM stream)

## 3. Create Chat Pipeline

Repeat the above steps to create a second pipeline named `tf2_chat` with stream schema `tf2-chat-stream.schema.json` and sink table `messages`.

## 4. Create Players Pipeline

Repeat the above steps to create a third pipeline named `tf2_players` with stream schema `tf2-player-stream.schema.json` and sink table `summaries`.

## 5. Configure ingest service bindings

Bind each pipeline stream to the ingest Worker:

- `TF2_LOGS_STREAM` -> core logs stream
- `TF2_CHAT_STREAM` -> chat messages stream
- `TF2_PLAYERS_STREAM` -> player summaries stream

Get stream IDs from the Cloudflare dashboard:

- Pipelines -> Streams -> open each stream
- copy the stream ID shown in the stream details page

Then set `apps/ingest-service/wrangler.jsonc`:

```jsonc
"pipelines": [
  { "binding": "TF2_LOGS_STREAM", "pipeline": "<TF2_LOGS_STREAM_ID>" },
  { "binding": "TF2_CHAT_STREAM", "pipeline": "<TF2_CHAT_STREAM_ID>" },
  { "binding": "TF2_PLAYERS_STREAM", "pipeline": "<TF2_PLAYERS_STREAM_ID>" }
]
```

Optional CLI alternative (if you want it later): `npx wrangler pipelines streams list --json`.

import {
  readIngestState,
  runIngest,
  type FullHistoryQueueMessage,
  type IngestEnv,
  type QueueBatch,
} from "./ingest";

interface ExecutionContextLike {
  waitUntil(promise: Promise<unknown>): void;
}

function jsonResponse(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
    },
  });
}

function parseOffsetParam(value: string | null): number {
  if (value === null) {
    return 0;
  }

  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return 0;
  }

  return parsed;
}

function asFullHistoryQueueMessage(value: unknown): FullHistoryQueueMessage | null {
  if (typeof value !== "object" || value === null) {
    return null;
  }

  const candidate = value as Partial<FullHistoryQueueMessage>;
  if (candidate.mode !== "full-history") {
    return null;
  }

  const offset = Number(candidate.offset);
  if (!Number.isFinite(offset) || offset < 0) {
    return null;
  }

  return {
    mode: "full-history",
    offset,
  };
}

async function enqueueFullHistoryOffset(env: IngestEnv, offset: number): Promise<void> {
  if (!env.INGEST_BACKFILL_QUEUE) {
    throw new Error("Missing required queue binding: INGEST_BACKFILL_QUEUE");
  }

  await env.INGEST_BACKFILL_QUEUE.send({
    mode: "full-history",
    offset,
  });
}

export default {
  async scheduled(_event: unknown, env: IngestEnv, ctx: ExecutionContextLike): Promise<void> {
    ctx.waitUntil(
      runIngest(env, { trigger: "scheduled" })
        .then((result) => {
          console.info("Ingest run complete", result);
        })
        .catch((error) => {
          console.error("Ingest run failed", error);
          throw error;
        }),
    );
  },

  async queue(batch: QueueBatch<unknown>, env: IngestEnv): Promise<void> {
    for (const message of batch.messages) {
      const parsed = asFullHistoryQueueMessage(message.body);
      if (!parsed) {
        console.error("Dropping invalid full-history queue message", message.body);
        continue;
      }

      const result = await runIngest(env, {
        mode: "full-history",
        fullHistoryOffset: parsed.offset,
        trigger: "queue",
      });

      console.info("Completed full-history ingest chunk", {
        offset: parsed.offset,
        result,
      });

      if (result.nextBackfillOffset !== null) {
        await enqueueFullHistoryOffset(env, result.nextBackfillOffset);
        console.info("Queued next full-history ingest chunk", {
          nextOffset: result.nextBackfillOffset,
        });
      }
    }
  },

  async fetch(request: Request, env: IngestEnv): Promise<Response> {
    const url = new URL(request.url);

    if (request.method === "GET" && url.pathname === "/health") {
      const state = await readIngestState(env);
      return jsonResponse(200, {
        ok: true,
        state,
      });
    }

    if (request.method === "POST" && url.pathname === "/ingest/full-history/start") {
      try {
        const offset = parseOffsetParam(url.searchParams.get("offset"));
        await enqueueFullHistoryOffset(env, offset);
        return jsonResponse(202, {
          ok: true,
          mode: "full-history",
          message: "Queued full-history ingest run.",
          offset,
        });
      } catch (error) {
        return jsonResponse(500, {
          ok: false,
          message: error instanceof Error ? error.message : String(error),
        });
      }
    }

    if (url.pathname === "/ingest" && (request.method === "GET" || request.method === "POST")) {
      const dryRun = url.searchParams.get("dryRun") === "true";
      const mode = url.searchParams.get("mode") === "full-history" ? "full-history" : "incremental";
      const fullHistoryOffset = parseOffsetParam(url.searchParams.get("offset"));

      if (request.method === "GET" && !dryRun) {
        return jsonResponse(405, {
          ok: false,
          message: "GET /ingest requires dryRun=true. Use POST /ingest for real ingestion.",
        });
      }

      try {
        const result = await runIngest(env, {
          dryRun,
          mode,
          fullHistoryOffset: mode === "full-history" ? fullHistoryOffset : undefined,
          trigger: "http",
        });
        return jsonResponse(200, {
          ok: true,
          dryRun,
          mode,
          result,
        });
      } catch (error) {
        return jsonResponse(500, {
          ok: false,
          message: error instanceof Error ? error.message : String(error),
        });
      }
    }

    return jsonResponse(404, {
      ok: false,
      message:
        "Use GET /health, GET /ingest?dryRun=true, POST /ingest, or POST /ingest/full-history/start",
    });
  },
};

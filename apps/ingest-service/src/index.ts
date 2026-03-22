import { readIngestState, runIngest, type IngestEnv } from "./ingest";

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

export default {
  async scheduled(_event: unknown, env: IngestEnv, ctx: ExecutionContextLike): Promise<void> {
    ctx.waitUntil(
      runIngest(env)
        .then((result) => {
          console.info("Ingest run complete", result);
        })
        .catch((error) => {
          console.error("Ingest run failed", error);
          throw error;
        }),
    );
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

    if (url.pathname === "/ingest" && (request.method === "GET" || request.method === "POST")) {
      const dryRun = url.searchParams.get("dryRun") === "true";

      if (request.method === "GET" && !dryRun) {
        return jsonResponse(405, {
          ok: false,
          message: "GET /ingest requires dryRun=true. Use POST /ingest for real ingestion.",
        });
      }

      try {
        const result = await runIngest(env, { dryRun });
        return jsonResponse(200, {
          ok: true,
          dryRun,
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
      message: "Use GET /health, GET /ingest?dryRun=true, or POST /ingest",
    });
  },
};

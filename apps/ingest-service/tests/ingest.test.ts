import { expect, test } from "vite-plus/test";
import { computeRetryDelayMs, runIngest, type IngestEnv, type KeyValueStore } from "../src/ingest";

class MemoryKv implements KeyValueStore {
  private values = new Map<string, string>();

  async get(key: string): Promise<string | null> {
    return this.values.get(key) ?? null;
  }

  async put(key: string, value: string): Promise<void> {
    this.values.set(key, value);
  }
}

class MemoryStream<TRecord> {
  readonly batches: TRecord[][] = [];

  async send(records: TRecord[]): Promise<void> {
    this.batches.push(records);
  }
}

function listPayload(logIds: number[]): unknown {
  return {
    success: true,
    results: logIds.length,
    total: 4_010_879,
    parameters: {},
    logs: logIds.map((id) => ({
      id,
      title: `log-${id}`,
      map: "cp_snakewater_final1",
      date: 1774213787,
      views: 0,
      players: 13,
    })),
  };
}

function detailPayload(logId: number): unknown {
  return {
    success: true,
    version: 3,
    length: 1423,
    info: {
      map: "cp_snakewater_final1",
      date: 1774213787,
      title: `log-${logId}`,
      uploader: {
        id: "76561197960497430",
        name: "Arie - VanillaTF2.org",
      },
    },
    teams: {
      Red: { score: 2 },
      Blue: { score: 5 },
    },
    chat: [
      { steamid: "[U:1:1]", name: "Player A", msg: "holy spam" },
      { steamid: "[U:1:2]", name: "Player B", msg: "gg" },
    ],
    players: {
      "[U:1:1]": {
        team: "Red",
        kills: 10,
        assists: 4,
        deaths: 6,
        dmg: 2500,
        dt: 1800,
        heal: 300,
        ubers: 1,
        class_stats: [{ type: "soldier" }],
      },
    },
  };
}

function makeFetchStub(calls: string[]): typeof fetch {
  return async (input: RequestInfo | URL): Promise<Response> => {
    const url =
      typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
    calls.push(url);

    if (url.includes("/logs?")) {
      return Response.json(listPayload([4031052, 4031051]));
    }

    const detailMatch = url.match(/\/log\/(\d+)$/);
    if (detailMatch) {
      return Response.json(detailPayload(Number(detailMatch[1])));
    }

    return new Response("not found", { status: 404 });
  };
}

test("computeRetryDelayMs exponentially backs off with max clamp", () => {
  expect(computeRetryDelayMs(1)).toBe(10_000);
  expect(computeRetryDelayMs(2)).toBe(20_000);
  expect(computeRetryDelayMs(20)).toBe(21_600_000);
});

test("runIngest fetches new logs and emits them", async () => {
  const kv = new MemoryKv();
  const logsStream = new MemoryStream<unknown>();
  const chatStream = new MemoryStream<unknown>();
  const playersStream = new MemoryStream<unknown>();
  const calls: string[] = [];

  const env: IngestEnv = {
    INGEST_CURSOR_KV: kv,
    TF2_LOGS_STREAM: logsStream,
    TF2_CHAT_STREAM: chatStream,
    TF2_PLAYERS_STREAM: playersStream,
    LOGS_TF_PAGE_SIZE: "50",
    LOGS_TF_MAX_PAGES_PER_RUN: "1",
    LOGS_TF_REQUEST_DELAY_MS: "1",
    PIPELINES_BATCH_SIZE: "10",
  };

  const result = await runIngest(env, {
    fetchFn: makeFetchStub(calls),
    now: new Date("2026-03-22T12:00:00.000Z"),
  });

  expect(result.fetchedNewLogs).toBe(2);
  expect(result.emittedLogs).toBe(2);
  expect(result.emittedCoreLogs).toBe(2);
  expect(result.emittedChatMessages).toBe(4);
  expect(result.emittedPlayerSummaries).toBe(2);
  expect(result.lastIngestedLogId).toBe(4031052);
  expect(calls.some((url) => url.includes("/logs?"))).toBe(true);
  expect(calls.some((url) => url.includes("/log/4031052"))).toBe(true);
  expect(logsStream.batches.flat()).toHaveLength(2);
  expect(chatStream.batches.flat()).toHaveLength(4);
  expect(playersStream.batches.flat()).toHaveLength(2);
});

test("runIngest fails fast when required logs binding is missing", async () => {
  const kv = new MemoryKv();
  const env = {
    INGEST_CURSOR_KV: kv,
  } as unknown as IngestEnv;

  await expect(
    runIngest(env, {
      fetchFn: makeFetchStub([]),
    }),
  ).rejects.toThrow(/TF2_LOGS_STREAM/);
});

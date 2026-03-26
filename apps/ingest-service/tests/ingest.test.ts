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
const CURSOR_STATE_KEY = "tf2-ingest-state-v1";

class MemoryStream<TRecord> {
  readonly batches: TRecord[][] = [];

  async send(records: TRecord[]): Promise<void> {
    this.batches.push(records);
  }
}

class FlakyMemoryStream<TRecord> extends MemoryStream<TRecord> {
  private failedAttempts = 0;

  constructor(
    private readonly shouldFail: (records: TRecord[]) => boolean,
    private readonly maxFailures = 1,
  ) {
    super();
  }

  async send(records: TRecord[]): Promise<void> {
    if (this.shouldFail(records) && this.failedAttempts < this.maxFailures) {
      this.failedAttempts += 1;
      throw new Error("CF_PIPELINE_DURABLE_OBJECT_OVERLOADED: Too many requests");
    }

    await super.send(records);
  }
}

class FailOnceMemoryStream<TRecord> extends MemoryStream<TRecord> {
  private hasFailed = false;

  async send(records: TRecord[]): Promise<void> {
    if (!this.hasFailed) {
      this.hasFailed = true;
      throw new Error("hard pipeline send failure");
    }
    await super.send(records);
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

function detailPayloadWithChat(logId: number, chatCount: number): unknown {
  const base = detailPayload(logId) as { chat?: unknown[] };
  return {
    ...base,
    chat: Array.from({ length: chatCount }, (_, index) => ({
      steamid: `[U:1:${index + 1}]`,
      name: `Player ${index + 1}`,
      msg: `message ${index + 1}`,
    })),
  };
}

function makeFetchStub(calls: string[]): typeof fetch {
  return async (input: RequestInfo | URL): Promise<Response> => {
    const url =
      typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
    calls.push(url);

    if (url.includes("/log?")) {
      return Response.json(listPayload([4031052, 4031051]));
    }

    const detailMatch = url.match(/\/log\/(\d+)$/);
    if (detailMatch) {
      return Response.json(detailPayload(Number(detailMatch[1])));
    }

    return new Response("not found", { status: 404 });
  };
}

function makeFullHistoryFetchStub(calls: string[]): typeof fetch {
  return async (input: RequestInfo | URL): Promise<Response> => {
    const url =
      typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
    calls.push(url);

    const parsed = new URL(url);
    const offset = Number(parsed.searchParams.get("offset") ?? "0");

    if (parsed.pathname.endsWith("/log")) {
      if (offset === 0) {
        return Response.json(listPayload([4031052, 4031051]));
      }
      if (offset === 2) {
        return Response.json(listPayload([4031050]));
      }
      return Response.json(listPayload([]));
    }

    const detailMatch = url.match(/\/log\/(\d+)$/);
    if (detailMatch) {
      return Response.json(detailPayload(Number(detailMatch[1])));
    }

    return new Response("not found", { status: 404 });
  };
}

function makeFetchStubWithDetail(
  calls: string[],
  listLogIds: number[],
  detailFactory: (logId: number) => unknown,
): typeof fetch {
  return async (input: RequestInfo | URL): Promise<Response> => {
    const url =
      typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
    calls.push(url);

    if (url.includes("/log?")) {
      return Response.json(listPayload(listLogIds));
    }

    const detailMatch = url.match(/\/log\/(\d+)$/);
    if (detailMatch) {
      return Response.json(detailFactory(Number(detailMatch[1])));
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

  expect(result.mode).toBe("incremental");
  expect(result.fetchedNewLogs).toBe(2);
  expect(result.emittedLogs).toBe(2);
  expect(result.emittedCoreLogs).toBe(2);
  expect(result.emittedChatMessages).toBe(4);
  expect(result.emittedPlayerSummaries).toBe(2);
  expect(result.lastIngestedLogId).toBe(4031052);
  expect(result.nextBackfillOffset).toBe(null);
  expect(calls.some((url) => url.includes("/log?"))).toBe(true);
  expect(calls.some((url) => url.includes("/log/4031052"))).toBe(true);
  expect(logsStream.batches.flat()).toHaveLength(2);
  expect(chatStream.batches.flat()).toHaveLength(4);
  expect(playersStream.batches.flat()).toHaveLength(2);
});

test("runIngest full-history mode returns a continuation offset when more pages exist", async () => {
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
    LOGS_TF_PAGE_SIZE: "2",
    LOGS_TF_MAX_PAGES_PER_RUN: "1",
    LOGS_TF_REQUEST_DELAY_MS: "1",
    PIPELINES_BATCH_SIZE: "10",
  };

  const first = await runIngest(env, {
    mode: "full-history",
    fullHistoryOffset: 0,
    fetchFn: makeFullHistoryFetchStub(calls),
    now: new Date("2026-03-22T12:00:00.000Z"),
  });

  expect(first.mode).toBe("full-history");
  expect(first.fetchedNewLogs).toBe(2);
  expect(first.nextBackfillOffset).toBe(2);
  expect(calls.some((url) => url.includes("offset=0"))).toBe(true);
  expect(calls.some((url) => url.includes("/log/4031052"))).toBe(true);
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

test("runIngest splits large per-log chat payloads into smaller sends", async () => {
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
    fetchFn: makeFetchStubWithDetail(calls, [4031052], (logId) => detailPayloadWithChat(logId, 25)),
    now: new Date("2026-03-22T12:00:00.000Z"),
  });

  expect(result.emittedCoreLogs).toBe(1);
  expect(result.emittedChatMessages).toBe(25);
  expect(result.emittedPlayerSummaries).toBe(1);
  expect(chatStream.batches.map((batch) => batch.length)).toEqual([10, 10, 5]);
});

test("runIngest retries transient pipeline overload errors", async () => {
  const kv = new MemoryKv();
  const logsStream = new FlakyMemoryStream<unknown>(() => true, 1);
  const chatStream = new MemoryStream<unknown>();
  const playersStream = new MemoryStream<unknown>();

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
    fetchFn: makeFetchStub([]),
    now: new Date("2026-03-22T12:00:00.000Z"),
  });

  expect(result.emittedCoreLogs).toBe(2);
  expect(result.failedLogs).toBe(0);
  expect(logsStream.batches.flat()).toHaveLength(2);
});

test("runIngest suppresses failed logs from new candidates until retry is due", async () => {
  const kv = new MemoryKv();
  const logsStream = new MemoryStream<unknown>();
  const chatStream = new MemoryStream<unknown>();
  const playersStream = new MemoryStream<unknown>();
  const calls: string[] = [];

  await kv.put(
    CURSOR_STATE_KEY,
    JSON.stringify({
      lastIngestedLogId: 4031050,
      failedLogs: {
        "4031052": {
          summary: {
            id: 4031052,
            title: "log-4031052",
            map: "cp_snakewater_final1",
            date: 1774213787,
            views: 0,
            players: 13,
          },
          attempts: 1,
          nextAttemptAtEpochMs: Date.parse("2026-03-22T12:10:00.000Z"),
          lastError: "temporary failure",
        },
      },
      updatedAt: "2026-03-22T12:00:00.000Z",
    }),
  );

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

  expect(result.emittedCoreLogs).toBe(1);
  expect(result.failedLogs).toBe(1);
  expect(logsStream.batches.flat()).toHaveLength(1);
  expect(calls.some((url) => url.includes("/log/4031052"))).toBe(false);
  expect(calls.some((url) => url.includes("/log/4031051"))).toBe(true);
});

test("runIngest resumes failed logs without re-emitting already delivered datasets", async () => {
  const kv = new MemoryKv();
  const logsStream = new MemoryStream<unknown>();
  const chatStream = new MemoryStream<unknown>();
  const playersStream = new FailOnceMemoryStream<unknown>();

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

  const first = await runIngest(env, {
    fetchFn: makeFetchStubWithDetail([], [4031052], (logId) => detailPayload(logId)),
    now: new Date("2026-03-22T12:00:00.000Z"),
  });

  expect(first.emittedCoreLogs).toBe(1);
  expect(first.emittedChatMessages).toBe(2);
  expect(first.emittedPlayerSummaries).toBe(0);
  expect(first.failedLogs).toBe(1);
  expect(first.lastIngestedLogId).toBe(0);

  const second = await runIngest(env, {
    fetchFn: makeFetchStubWithDetail([], [4031052], (logId) => detailPayload(logId)),
    now: new Date("2026-03-22T12:00:11.000Z"),
  });

  expect(second.emittedCoreLogs).toBe(0);
  expect(second.emittedChatMessages).toBe(0);
  expect(second.emittedPlayerSummaries).toBe(1);
  expect(second.failedLogs).toBe(0);
  expect(second.lastIngestedLogId).toBe(4031052);

  expect(logsStream.batches.flat()).toHaveLength(1);
  expect(chatStream.batches.flat()).toHaveLength(2);
  expect(playersStream.batches.flat()).toHaveLength(1);
});

test("runIngest caps due retries per run", async () => {
  const kv = new MemoryKv();
  const logsStream = new MemoryStream<unknown>();
  const chatStream = new MemoryStream<unknown>();
  const playersStream = new MemoryStream<unknown>();
  const calls: string[] = [];

  await kv.put(
    CURSOR_STATE_KEY,
    JSON.stringify({
      lastIngestedLogId: 4031052,
      failedLogs: {
        "3905184": {
          summary: {
            id: 3905184,
            title: "failed-3905184",
            map: "cp_process_f12",
            date: 1774213000,
            views: 10,
            players: 12,
          },
          attempts: 3,
          nextAttemptAtEpochMs: 0,
          lastError: "temporary failure",
        },
        "3905192": {
          summary: {
            id: 3905192,
            title: "failed-3905192",
            map: "cp_process_f12",
            date: 1774213000,
            views: 10,
            players: 12,
          },
          attempts: 3,
          nextAttemptAtEpochMs: 0,
          lastError: "temporary failure",
        },
        "3905217": {
          summary: {
            id: 3905217,
            title: "failed-3905217",
            map: "cp_process_f12",
            date: 1774213000,
            views: 10,
            players: 12,
          },
          attempts: 3,
          nextAttemptAtEpochMs: 0,
          lastError: "temporary failure",
        },
      },
      updatedAt: "2026-03-22T12:00:00.000Z",
    }),
  );

  const env: IngestEnv = {
    INGEST_CURSOR_KV: kv,
    TF2_LOGS_STREAM: logsStream,
    TF2_CHAT_STREAM: chatStream,
    TF2_PLAYERS_STREAM: playersStream,
    LOGS_TF_PAGE_SIZE: "50",
    LOGS_TF_MAX_PAGES_PER_RUN: "1",
    LOGS_TF_REQUEST_DELAY_MS: "1",
    LOGS_TF_MAX_RETRY_LOGS_PER_RUN: "2",
    PIPELINES_BATCH_SIZE: "10",
  };

  const result = await runIngest(env, {
    fetchFn: makeFetchStubWithDetail(calls, [], (logId) => detailPayload(logId)),
    now: new Date("2026-03-22T12:05:00.000Z"),
  });

  expect(result.retriedLogs).toBe(2);
  expect(result.failedLogs).toBe(1);
  expect(logsStream.batches.flat()).toHaveLength(2);
});

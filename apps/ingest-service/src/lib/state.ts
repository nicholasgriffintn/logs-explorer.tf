import type { LogsTfLogSummary } from "@logs-explorer/tf2-log-model";
import type { FailedLogState, IngestEnv, IngestState } from "./types";

const CURSOR_STATE_KEY = "tf2-ingest-state-v1";

function defaultState(nowIso: string): IngestState {
  return {
    lastIngestedLogId: 0,
    failedLogs: {},
    updatedAt: nowIso,
  };
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function parseFailedSummary(value: unknown): LogsTfLogSummary | null {
  if (!isObject(value)) {
    return null;
  }

  const parsedSummary: LogsTfLogSummary = {
    id: Number(value.id),
    title: String(value.title),
    map: String(value.map),
    date: Number(value.date),
    views: Number(value.views),
    players: Number(value.players),
  };

  if (
    !Number.isFinite(parsedSummary.id) ||
    !Number.isFinite(parsedSummary.date) ||
    !Number.isFinite(parsedSummary.views) ||
    !Number.isFinite(parsedSummary.players)
  ) {
    return null;
  }

  return parsedSummary;
}

function toFailedLogState(value: unknown): FailedLogState | null {
  if (!isObject(value)) {
    return null;
  }

  const parsedSummary = parseFailedSummary(value.summary);
  if (!parsedSummary) {
    return null;
  }

  return {
    summary: parsedSummary,
    attempts: Number(value.attempts),
    nextAttemptAtEpochMs: Number(value.nextAttemptAtEpochMs),
    lastError: String(value.lastError),
  };
}

function parseState(raw: string | null, nowIso: string): IngestState {
  if (!raw) {
    return defaultState(nowIso);
  }

  try {
    const value = JSON.parse(raw) as unknown;
    if (!isObject(value)) {
      return defaultState(nowIso);
    }

    const failedLogs: Record<string, FailedLogState> = {};
    if (isObject(value.failedLogs)) {
      for (const [logId, failed] of Object.entries(value.failedLogs)) {
        const parsed = toFailedLogState(failed);
        if (parsed !== null && Number.isFinite(parsed.summary.id)) {
          failedLogs[logId] = parsed;
        }
      }
    }

    return {
      lastIngestedLogId: Number.isFinite(Number(value.lastIngestedLogId))
        ? Number(value.lastIngestedLogId)
        : 0,
      failedLogs,
      updatedAt: typeof value.updatedAt === "string" ? value.updatedAt : nowIso,
    };
  } catch {
    return defaultState(nowIso);
  }
}

export async function readState(env: IngestEnv, nowIso: string): Promise<IngestState> {
  const raw = await env.INGEST_CURSOR_KV.get(CURSOR_STATE_KEY);
  return parseState(raw, nowIso);
}

export async function writeState(env: IngestEnv, state: IngestState): Promise<void> {
  await env.INGEST_CURSOR_KV.put(CURSOR_STATE_KEY, JSON.stringify(state));
}

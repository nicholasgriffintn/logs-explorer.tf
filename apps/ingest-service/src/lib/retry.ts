import {
  dedupeSummariesByLogId,
  sortSummariesAscending,
  type LogsTfLogSummary,
} from "@logs-explorer/tf2-log-model";
import type { IngestState } from "./types";

export function computeRetryDelayMs(attempts: number): number {
  const unclamped = 10_000 * 2 ** Math.max(0, attempts - 1);
  return Math.min(unclamped, 6 * 60 * 60 * 1000);
}

export function dueRetrySummaries(state: IngestState, nowEpochMs: number): LogsTfLogSummary[] {
  const due: LogsTfLogSummary[] = [];

  for (const failed of Object.values(state.failedLogs)) {
    if (failed.nextAttemptAtEpochMs <= nowEpochMs) {
      due.push(failed.summary);
    }
  }

  return due;
}

export function mergeCandidates(
  newSummaries: LogsTfLogSummary[],
  retries: LogsTfLogSummary[],
): LogsTfLogSummary[] {
  return sortSummariesAscending(dedupeSummariesByLogId([...retries, ...newSummaries]));
}

export function updateFailure(
  state: IngestState,
  summary: LogsTfLogSummary,
  message: string,
  nowEpochMs: number,
): void {
  const key = String(summary.id);
  const existing = state.failedLogs[key];
  const attempts = (existing?.attempts ?? 0) + 1;

  state.failedLogs[key] = {
    summary,
    attempts,
    nextAttemptAtEpochMs: nowEpochMs + computeRetryDelayMs(attempts),
    lastError: message,
  };
}

export function clearFailure(state: IngestState, logId: number): void {
  delete state.failedLogs[String(logId)];
}

export function pruneFailures(state: IngestState, maxFailedLogs: number): void {
  const entries = Object.entries(state.failedLogs);
  if (entries.length <= maxFailedLogs) {
    return;
  }

  entries.sort((left, right) => left[1].nextAttemptAtEpochMs - right[1].nextAttemptAtEpochMs);

  for (const [logId] of entries.slice(maxFailedLogs)) {
    delete state.failedLogs[logId];
  }
}

import {
  extractChatMessageRecords,
  extractPlayerSummaryRecords,
  normalizeLogRecord,
  parseLogDetailResponse,
  type LogsTfLogSummary,
} from "@logs-explorer/tf2-log-model";

import { buildConfig } from "./lib/config";
import { emitDatasetBatch } from "./lib/emitter";
import { fetchJsonWithRetry, toDetailUrl } from "./lib/http";
import {
  clearFailure,
  deliveryStateFromFailure,
  computeRetryDelayMs,
  dueRetrySummaries,
  mergeCandidates,
  pruneFailures,
  updateFailure,
} from "./lib/retry";
import { collectFullHistorySummaries, collectNewSummaries } from "./lib/source";
import { readState, writeState } from "./lib/state";
import { sleep } from "./lib/time";
import type {
  IngestMode,
  IngestEnv,
  IngestResult,
  IngestState,
  KeyValueStore,
  RunIngestOptions,
} from "./lib/types";

export type {
  FailedLogState,
  FullHistoryQueueMessage,
  IngestConfig,
  IngestMode,
  PipelinesStreamBinding,
  QueueBatch,
  QueueBinding,
} from "./lib/types";
export type { IngestEnv, IngestResult, IngestState, KeyValueStore, RunIngestOptions };

export { computeRetryDelayMs };

function assertRequiredBindings(env: IngestEnv): void {
  if (!env.TF2_LOGS_STREAM) {
    throw new Error("Missing required pipeline binding: TF2_LOGS_STREAM");
  }
  if (!env.TF2_CHAT_STREAM) {
    throw new Error("Missing required pipeline binding: TF2_CHAT_STREAM");
  }
  if (!env.TF2_PLAYERS_STREAM) {
    throw new Error("Missing required pipeline binding: TF2_PLAYERS_STREAM");
  }
}

export async function runIngest(
  env: IngestEnv,
  options: RunIngestOptions = {},
): Promise<IngestResult> {
  if (!options.dryRun) {
    assertRequiredBindings(env);
  }

  console.log("Starting ingest run with options", options);

  const now = options.now ?? new Date();
  const nowIso = now.toISOString();
  const nowEpochMs = now.getTime();
  const mode: IngestMode = options.mode ?? "incremental";
  const fetchFn = options.fetchFn ?? fetch;
  const config = buildConfig(env);
  const state = await readState(env, nowIso);

  console.log("Current ingest state", {
    lastIngestedLogId: state.lastIngestedLogId,
    failedLogs: Object.keys(state.failedLogs).length,
    updatedAt: state.updatedAt,
  });

  let newSummaries: LogsTfLogSummary[] = [];
  let retrySummaries: LogsTfLogSummary[] = [];
  let candidates: LogsTfLogSummary[] = [];
  let nextBackfillOffset: number | null = null;

  if (mode === "full-history") {
    const offset = Math.max(0, options.fullHistoryOffset ?? 0);
    const fullHistoryBatch = await collectFullHistorySummaries(fetchFn, config, offset);
    newSummaries = fullHistoryBatch.summaries;
    candidates = fullHistoryBatch.summaries;
    nextBackfillOffset = fullHistoryBatch.nextOffset;
  } else {
    const fetchedNewSummaries = await collectNewSummaries(fetchFn, config, state.lastIngestedLogId);
    const failedLogIds = new Set(Object.keys(state.failedLogs).map((id) => Number(id)));
    newSummaries = fetchedNewSummaries.filter((summary) => !failedLogIds.has(summary.id));

    const suppressedPendingFailures = fetchedNewSummaries.length - newSummaries.length;
    if (suppressedPendingFailures > 0) {
      console.log(
        `Suppressed ${suppressedPendingFailures} failed logs from new-candidate selection until retry backoff allows them`,
      );
    }

    const dueRetries = dueRetrySummaries(state, nowEpochMs);
    retrySummaries = dueRetries.slice(0, config.maxRetryLogsPerRun);
    if (dueRetries.length > retrySummaries.length) {
      console.log(
        `Retry backlog throttled: processing ${retrySummaries.length} of ${dueRetries.length} due retries this run`,
      );
    }
    candidates = mergeCandidates(newSummaries, retrySummaries);
  }

  const candidatePreviewLimit = 50;
  const candidateIds = candidates.slice(0, candidatePreviewLimit).map((s) => s.id);

  console.log(
    `Mode=${mode}. Fetched ${newSummaries.length} new summaries and ${retrySummaries.length} retry summaries, total ${candidates.length} candidates to process`,
    candidateIds,
  );
  if (candidates.length > candidatePreviewLimit) {
    console.log(
      `Candidate list truncated in logs: ${candidates.length - candidatePreviewLimit} more`,
    );
  }

  let emittedCoreLogs = 0;
  let emittedChatMessages = 0;
  let emittedPlayerSummaries = 0;

  if (!options.dryRun) {
    console.log(`Emitting logs one at a time with per-send record cap ${config.pipelineBatchSize}`);
  }

  for (const summary of candidates) {
    const deliveredDatasets = deliveryStateFromFailure(state.failedLogs[String(summary.id)]);

    try {
      const rawDetail = await fetchJsonWithRetry(
        fetchFn,
        toDetailUrl(config, summary.id),
        config.fetchRetries,
        config.requestDelayMs,
      );
      const detail = parseLogDetailResponse(rawDetail);
      const coreRecord = normalizeLogRecord(summary, detail, nowIso);
      const chatRecords = extractChatMessageRecords(summary, detail, nowIso);
      const playerRecords = extractPlayerSummaryRecords(summary, detail, nowIso);

      if (!options.dryRun) {
        if (!deliveredDatasets.logs) {
          const emittedCoreForLog = await emitDatasetBatch(
            "logs",
            env,
            [coreRecord],
            config.pipelineBatchSize,
          );
          emittedCoreLogs += emittedCoreForLog;
          deliveredDatasets.logs = true;
        }

        if (!deliveredDatasets.chat) {
          const emittedChatForLog = await emitDatasetBatch(
            "chat",
            env,
            chatRecords,
            config.pipelineBatchSize,
          );
          emittedChatMessages += emittedChatForLog;
          deliveredDatasets.chat = true;
        }

        if (!deliveredDatasets.players) {
          const emittedPlayersForLog = await emitDatasetBatch(
            "players",
            env,
            playerRecords,
            config.pipelineBatchSize,
          );
          emittedPlayerSummaries += emittedPlayersForLog;
          deliveredDatasets.players = true;
        }
      }

      if (!options.dryRun) {
        clearFailure(state, summary.id);
        state.lastIngestedLogId = Math.max(state.lastIngestedLogId, summary.id);
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.error(`Failed to process log ${summary.id}: ${message}`);
      updateFailure(state, summary, message, nowEpochMs, deliveredDatasets);
    }

    await sleep(config.requestDelayMs);
  }

  state.updatedAt = nowIso;
  pruneFailures(state, config.maxFailedLogs);
  await writeState(env, state);

  console.log("Ingest run complete");

  return {
    mode,
    fetchedNewLogs: newSummaries.length,
    retriedLogs: retrySummaries.length,
    emittedLogs: emittedCoreLogs,
    emittedCoreLogs,
    emittedChatMessages,
    emittedPlayerSummaries,
    failedLogs: Object.keys(state.failedLogs).length,
    lastIngestedLogId: state.lastIngestedLogId,
    nextBackfillOffset,
  };
}

export async function readIngestState(env: IngestEnv, now = new Date()): Promise<IngestState> {
  return readState(env, now.toISOString());
}

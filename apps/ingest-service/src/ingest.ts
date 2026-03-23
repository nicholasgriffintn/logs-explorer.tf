import {
  extractChatMessageRecords,
  extractPlayerSummaryRecords,
  normalizeLogRecord,
  parseLogDetailResponse,
  type ChatMessageRecord,
  type LogsTfLogSummary,
  type NormalizedLogRecord,
  type PlayerSummaryRecord,
} from "@logs-explorer/tf2-log-model";

import { buildConfig } from "./lib/config";
import { emitDatasetBatch } from "./lib/emitter";
import { fetchJsonWithRetry, toDetailUrl } from "./lib/http";
import {
  clearFailure,
  computeRetryDelayMs,
  dueRetrySummaries,
  mergeCandidates,
  pruneFailures,
  updateFailure,
} from "./lib/retry";
import { collectNewSummaries } from "./lib/source";
import { readState, writeState } from "./lib/state";
import { sleep } from "./lib/time";
import type {
  IngestEnv,
  IngestResult,
  IngestState,
  KeyValueStore,
  RunIngestOptions,
} from "./lib/types";

export type { FailedLogState, IngestConfig, PipelinesStreamBinding } from "./lib/types";
export type { IngestEnv, IngestResult, IngestState, KeyValueStore, RunIngestOptions };

interface PreparedLogItem {
  summary: LogsTfLogSummary;
  coreRecord: NormalizedLogRecord;
  chatRecords: ChatMessageRecord[];
  playerRecords: PlayerSummaryRecord[];
}

export { computeRetryDelayMs };

function prepareBatchableRecords(
  summaries: LogsTfLogSummary[],
  recordsByLogId: Map<number, PreparedLogItem>,
): PreparedLogItem[] {
  const ordered: PreparedLogItem[] = [];

  for (const summary of summaries) {
    const prepared = recordsByLogId.get(summary.id);
    if (prepared) {
      ordered.push(prepared);
    }
  }

  return ordered;
}

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
  const fetchFn = options.fetchFn ?? fetch;
  const config = buildConfig(env);
  const state = await readState(env, nowIso);

  console.log("Current ingest state", state);

  const newSummaries = await collectNewSummaries(fetchFn, config, state.lastIngestedLogId);
  const retrySummaries = dueRetrySummaries(state, nowEpochMs);
  const candidates = mergeCandidates(newSummaries, retrySummaries);

  const preparedByLogId = new Map<number, PreparedLogItem>();

  console.log(
    `Fetched ${newSummaries.length} new summaries and ${retrySummaries.length} retry summaries, total ${candidates.length} candidates to process`,
    candidates.map((s) => s.id),
  );

  for (const summary of candidates) {
    try {
      const rawDetail = await fetchJsonWithRetry(
        fetchFn,
        toDetailUrl(config, summary.id),
        config.fetchRetries,
        config.requestDelayMs,
      );
      const detail = parseLogDetailResponse(rawDetail);
      preparedByLogId.set(summary.id, {
        summary,
        coreRecord: normalizeLogRecord(summary, detail, nowIso),
        chatRecords: extractChatMessageRecords(summary, detail, nowIso),
        playerRecords: extractPlayerSummaryRecords(summary, detail, nowIso),
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.error(`Failed to fetch or parse log detail for log ${summary.id}: ${message}`);
      updateFailure(state, summary, message, nowEpochMs);
    }

    await sleep(config.requestDelayMs);
  }

  let emittedCoreLogs = 0;
  let emittedChatMessages = 0;
  let emittedPlayerSummaries = 0;

  if (!options.dryRun) {
    const preparedRecords = prepareBatchableRecords(candidates, preparedByLogId);

    console.log(
      `Prepared ${preparedRecords.length} records for emission, processing in batches of ${config.pipelineBatchSize}`,
    );

    for (let offset = 0; offset < preparedRecords.length; offset += config.pipelineBatchSize) {
      const batchItems = preparedRecords.slice(offset, offset + config.pipelineBatchSize);

      try {
        emittedCoreLogs += await emitDatasetBatch(
          "logs",
          env,
          batchItems.map((item) => item.coreRecord),
        );
        emittedChatMessages += await emitDatasetBatch(
          "chat",
          env,
          batchItems.flatMap((item) => item.chatRecords),
        );
        emittedPlayerSummaries += await emitDatasetBatch(
          "players",
          env,
          batchItems.flatMap((item) => item.playerRecords),
        );

        console.log(
          `Successfully emitted batch of ${batchItems.length} logs (offset ${offset}). Total emitted so far: ${emittedCoreLogs} core logs, ${emittedChatMessages} chat messages, ${emittedPlayerSummaries} player summaries.`,
        );

        for (const item of batchItems) {
          clearFailure(state, item.summary.id);
          state.lastIngestedLogId = Math.max(state.lastIngestedLogId, item.summary.id);
        }
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);

        console.error(`Failed to emit batch of logs (offset ${offset}): ${message}`);

        for (const item of batchItems) {
          updateFailure(state, item.summary, message, nowEpochMs);
        }
      }
    }
  }

  state.updatedAt = nowIso;
  pruneFailures(state, config.maxFailedLogs);
  await writeState(env, state);

  console.log("Ingest run complete");

  return {
    fetchedNewLogs: newSummaries.length,
    retriedLogs: retrySummaries.length,
    emittedLogs: emittedCoreLogs,
    emittedCoreLogs,
    emittedChatMessages,
    emittedPlayerSummaries,
    failedLogs: Object.keys(state.failedLogs).length,
    lastIngestedLogId: state.lastIngestedLogId,
  };
}

export async function readIngestState(env: IngestEnv, now = new Date()): Promise<IngestState> {
  return readState(env, now.toISOString());
}

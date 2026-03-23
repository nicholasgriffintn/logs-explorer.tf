import {
  dedupeSummariesByLogId,
  parseLogListResponse,
  sortSummariesAscending,
  type LogsTfLogSummary,
} from "@logs-explorer/tf2-log-model";
import type { IngestConfig } from "./types";
import { fetchJsonWithRetry, toListUrl } from "./http";
import { sleep } from "./time";

interface CollectSummariesOptions {
  startOffset: number;
  maxPages: number;
  stopAtLogId?: number;
}

interface CollectedSummaries {
  summaries: LogsTfLogSummary[];
  nextOffset: number | null;
}

export async function collectSummaries(
  fetchFn: typeof fetch,
  config: IngestConfig,
  options: CollectSummariesOptions,
): Promise<CollectedSummaries> {
  const collected: LogsTfLogSummary[] = [];
  let offset = options.startOffset;

  for (let page = 0; page < options.maxPages; page += 1) {
    const raw = await fetchJsonWithRetry(
      fetchFn,
      toListUrl(config, offset),
      config.fetchRetries,
      config.requestDelayMs,
    );
    const parsed = parseLogListResponse(raw);

    if (parsed.logs.length === 0) {
      return {
        summaries: sortSummariesAscending(dedupeSummariesByLogId(collected)),
        nextOffset: null,
      };
    }

    let reachedKnownBoundary = false;

    for (const summary of parsed.logs) {
      if (options.stopAtLogId !== undefined && summary.id <= options.stopAtLogId) {
        reachedKnownBoundary = true;
        break;
      }

      collected.push(summary);
    }

    if (reachedKnownBoundary || parsed.logs.length < config.pageSize) {
      return {
        summaries: sortSummariesAscending(dedupeSummariesByLogId(collected)),
        nextOffset: null,
      };
    }

    offset += config.pageSize;

    if (page < options.maxPages - 1) {
      await sleep(config.requestDelayMs);
    }
  }

  return {
    summaries: sortSummariesAscending(dedupeSummariesByLogId(collected)),
    nextOffset: offset,
  };
}

export async function collectNewSummaries(
  fetchFn: typeof fetch,
  config: IngestConfig,
  lastIngestedLogId: number,
): Promise<LogsTfLogSummary[]> {
  const { summaries } = await collectSummaries(fetchFn, config, {
    startOffset: 0,
    maxPages: config.maxPagesPerRun,
    stopAtLogId: lastIngestedLogId,
  });
  return summaries;
}

export async function collectFullHistorySummaries(
  fetchFn: typeof fetch,
  config: IngestConfig,
  startOffset: number,
): Promise<CollectedSummaries> {
  return collectSummaries(fetchFn, config, {
    startOffset,
    maxPages: config.maxPagesPerRun,
  });
}

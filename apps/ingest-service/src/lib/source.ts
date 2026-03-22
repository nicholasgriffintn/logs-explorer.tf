import {
  dedupeSummariesByLogId,
  parseLogListResponse,
  sortSummariesAscending,
  type LogsTfLogSummary,
} from "@logs-explorer/tf2-log-model";
import type { IngestConfig } from "./types";
import { fetchJsonWithRetry, toListUrl } from "./http";
import { sleep } from "./time";

export async function collectNewSummaries(
  fetchFn: typeof fetch,
  config: IngestConfig,
  lastIngestedLogId: number,
): Promise<LogsTfLogSummary[]> {
  const collected: LogsTfLogSummary[] = [];
  let offset = 0;

  for (let page = 0; page < config.maxPagesPerRun; page += 1) {
    const raw = await fetchJsonWithRetry(
      fetchFn,
      toListUrl(config, offset),
      config.fetchRetries,
      config.requestDelayMs,
    );
    const parsed = parseLogListResponse(raw);

    if (parsed.logs.length === 0) {
      break;
    }

    let reachedKnownBoundary = false;

    for (const summary of parsed.logs) {
      if (summary.id <= lastIngestedLogId) {
        reachedKnownBoundary = true;
        break;
      }

      collected.push(summary);
    }

    if (reachedKnownBoundary || parsed.logs.length < config.pageSize) {
      break;
    }

    offset += config.pageSize;
    await sleep(config.requestDelayMs);
  }

  return sortSummariesAscending(dedupeSummariesByLogId(collected));
}

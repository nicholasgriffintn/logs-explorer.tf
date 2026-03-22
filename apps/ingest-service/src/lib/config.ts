import type { IngestConfig, IngestEnv } from "./types";

function parsePositiveInt(value: string | undefined, fallback: number): number {
  if (!value) {
    return fallback;
  }

  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }

  return parsed;
}

export function buildConfig(env: IngestEnv): IngestConfig {
  return {
    apiBase: env.LOGS_TF_API_BASE ?? "https://logs.tf/api/v1",
    pageSize: parsePositiveInt(env.LOGS_TF_PAGE_SIZE, 200),
    maxPagesPerRun: parsePositiveInt(env.LOGS_TF_MAX_PAGES_PER_RUN, 5),
    requestDelayMs: parsePositiveInt(env.LOGS_TF_REQUEST_DELAY_MS, 250),
    fetchRetries: parsePositiveInt(env.LOGS_TF_FETCH_RETRIES, 4),
    maxFailedLogs: parsePositiveInt(env.LOGS_TF_MAX_FAILED_LOGS, 500),
    pipelineBatchSize: parsePositiveInt(env.PIPELINES_BATCH_SIZE, 50),
  };
}

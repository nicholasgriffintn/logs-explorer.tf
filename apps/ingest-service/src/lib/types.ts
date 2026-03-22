import type {
  ChatMessageRecord,
  LogsTfLogSummary,
  NormalizedLogRecord,
  PlayerSummaryRecord,
} from "@logs-explorer/tf2-log-model";

export interface KeyValueStore {
  get(key: string): Promise<string | null>;
  put(key: string, value: string): Promise<void>;
}

export interface PipelinesStreamBinding<TRecord> {
  send(records: TRecord[]): Promise<void>;
}

export interface IngestEnv {
  INGEST_CURSOR_KV: KeyValueStore;
  TF2_LOGS_STREAM: PipelinesStreamBinding<NormalizedLogRecord>;
  TF2_CHAT_STREAM: PipelinesStreamBinding<ChatMessageRecord>;
  TF2_PLAYERS_STREAM: PipelinesStreamBinding<PlayerSummaryRecord>;
  LOGS_TF_API_BASE?: string;
  LOGS_TF_PAGE_SIZE?: string;
  LOGS_TF_MAX_PAGES_PER_RUN?: string;
  LOGS_TF_REQUEST_DELAY_MS?: string;
  LOGS_TF_FETCH_RETRIES?: string;
  LOGS_TF_MAX_FAILED_LOGS?: string;
  PIPELINES_BATCH_SIZE?: string;
}

export interface FailedLogState {
  summary: LogsTfLogSummary;
  attempts: number;
  nextAttemptAtEpochMs: number;
  lastError: string;
}

export interface IngestState {
  lastIngestedLogId: number;
  failedLogs: Record<string, FailedLogState>;
  updatedAt: string;
}

export interface IngestResult {
  fetchedNewLogs: number;
  retriedLogs: number;
  emittedLogs: number;
  emittedCoreLogs: number;
  emittedChatMessages: number;
  emittedPlayerSummaries: number;
  failedLogs: number;
  lastIngestedLogId: number;
}

export interface IngestConfig {
  apiBase: string;
  pageSize: number;
  maxPagesPerRun: number;
  requestDelayMs: number;
  fetchRetries: number;
  maxFailedLogs: number;
  pipelineBatchSize: number;
}

export interface RunIngestOptions {
  dryRun?: boolean;
  now?: Date;
  fetchFn?: typeof fetch;
}

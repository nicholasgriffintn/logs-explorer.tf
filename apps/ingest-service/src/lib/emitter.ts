import type {
  ChatMessageRecord,
  NormalizedLogRecord,
  PlayerSummaryRecord,
} from "@logs-explorer/tf2-log-model";
import { sleep } from "./time";
import type { IngestEnv, PipelinesStreamBinding } from "./types";

export type DatasetKind = "logs" | "chat" | "players";
const MAX_SEND_ATTEMPTS = 3;
const SEND_RETRY_DELAY_MS = 500;

function datasetConfig(
  dataset: DatasetKind,
  env: IngestEnv,
): {
  binding: PipelinesStreamBinding<unknown>;
} {
  if (dataset === "logs") {
    return {
      binding: env.TF2_LOGS_STREAM as PipelinesStreamBinding<unknown>,
    };
  }

  if (dataset === "chat") {
    return {
      binding: env.TF2_CHAT_STREAM as PipelinesStreamBinding<unknown>,
    };
  }

  return {
    binding: env.TF2_PLAYERS_STREAM as PipelinesStreamBinding<unknown>,
  };
}

function isPipelineOverloadedError(message: string): boolean {
  return (
    message.includes("CF_PIPELINE_DURABLE_OBJECT_OVERLOADED") ||
    message.includes("Too many requests")
  );
}

async function sendWithRetry(
  dataset: DatasetKind,
  binding: PipelinesStreamBinding<unknown>,
  records: unknown[],
): Promise<void> {
  for (let attempt = 1; attempt <= MAX_SEND_ATTEMPTS; attempt += 1) {
    try {
      await binding.send(records);
      return;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      const shouldRetry = attempt < MAX_SEND_ATTEMPTS && isPipelineOverloadedError(message);

      if (!shouldRetry) {
        throw error;
      }

      const delayMs = SEND_RETRY_DELAY_MS * attempt;
      console.warn(
        `Pipeline overloaded while sending ${dataset} records (${records.length}). Retrying in ${delayMs}ms.`,
      );
      await sleep(delayMs);
    }
  }
}

export async function emitDatasetBatch(
  dataset: "logs",
  env: IngestEnv,
  records: NormalizedLogRecord[],
  maxRecordsPerSend: number,
): Promise<number>;
export async function emitDatasetBatch(
  dataset: "chat",
  env: IngestEnv,
  records: ChatMessageRecord[],
  maxRecordsPerSend: number,
): Promise<number>;
export async function emitDatasetBatch(
  dataset: "players",
  env: IngestEnv,
  records: PlayerSummaryRecord[],
  maxRecordsPerSend: number,
): Promise<number>;
export async function emitDatasetBatch(
  dataset: DatasetKind,
  env: IngestEnv,
  records: unknown[],
  maxRecordsPerSend: number,
): Promise<number> {
  if (records.length === 0) {
    return 0;
  }

  const recordsPerSend = Math.max(1, maxRecordsPerSend);
  const target = datasetConfig(dataset, env);

  for (let offset = 0; offset < records.length; offset += recordsPerSend) {
    const batch = records.slice(offset, offset + recordsPerSend);
    await sendWithRetry(dataset, target.binding, batch);
  }

  return records.length;
}

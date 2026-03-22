import type {
  ChatMessageRecord,
  NormalizedLogRecord,
  PlayerSummaryRecord,
} from "@logs-explorer/tf2-log-model";
import type { IngestEnv, PipelinesStreamBinding } from "./types";

export type DatasetKind = "logs" | "chat" | "players";

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

export async function emitDatasetBatch(
  dataset: "logs",
  env: IngestEnv,
  records: NormalizedLogRecord[],
): Promise<number>;
export async function emitDatasetBatch(
  dataset: "chat",
  env: IngestEnv,
  records: ChatMessageRecord[],
): Promise<number>;
export async function emitDatasetBatch(
  dataset: "players",
  env: IngestEnv,
  records: PlayerSummaryRecord[],
): Promise<number>;
export async function emitDatasetBatch(
  dataset: DatasetKind,
  env: IngestEnv,
  records: unknown[],
): Promise<number> {
  if (records.length === 0) {
    return 0;
  }

  const target = datasetConfig(dataset, env);

  await target.binding.send(records);
  return records.length;
}

import type {
  ChatMessageRecord,
  LogsTfLogDetail,
  LogsTfLogSummary,
  LogsTfPlayerDetail,
  PlayerSummaryRecord,
  NormalizedLogRecord,
} from "./types";

function epochToIso(epochSeconds: number): string {
  return new Date(epochSeconds * 1000).toISOString();
}

function toNullableNumber(value: unknown): number | null {
  return typeof value === "number" && !Number.isNaN(value) ? value : null;
}

export function normalizeLogRecord(
  summary: LogsTfLogSummary,
  detail: LogsTfLogDetail,
  ingestedAt: string,
): NormalizedLogRecord {
  const detailInfo = detail.info;
  const resolvedMap = detailInfo?.map ?? summary.map;
  const resolvedTitle = detailInfo?.title ?? summary.title;
  const resolvedDate = detailInfo?.date ?? summary.date;

  const uploader = detailInfo?.uploader;
  const uploaderId =
    uploader === undefined || uploader === null
      ? null
      : typeof uploader === "object"
        ? (uploader.id ?? null)
        : String(uploader);

  return {
    recordId: String(summary.id),
    logId: summary.id,
    title: resolvedTitle,
    map: resolvedMap,
    sourceDateEpochSeconds: resolvedDate,
    sourceDateIso: epochToIso(resolvedDate),
    sourcePlayerCount: summary.players,
    sourceViewCount: summary.views,
    durationSeconds: toNullableNumber(detail.length ?? detailInfo?.total_length),
    redScore: toNullableNumber(detail.teams?.Red?.score),
    blueScore: toNullableNumber(detail.teams?.Blue?.score),
    uploaderSteamId: uploaderId,
    payloadSchemaVersion: toNullableNumber(detail.version),
    ingestedAt,
  };
}

function resolveLogMetadata(
  summary: LogsTfLogSummary,
  detail: LogsTfLogDetail,
): {
  title: string;
  map: string;
  sourceDateEpochSeconds: number;
  sourceDateIso: string;
} {
  const detailInfo = detail.info;
  const sourceDateEpochSeconds = detailInfo?.date ?? summary.date;

  return {
    title: detailInfo?.title ?? summary.title,
    map: detailInfo?.map ?? summary.map,
    sourceDateEpochSeconds,
    sourceDateIso: epochToIso(sourceDateEpochSeconds),
  };
}

function normaliseMessage(value: string): string {
  return value.toLocaleLowerCase();
}

export function extractChatMessageRecords(
  summary: LogsTfLogSummary,
  detail: LogsTfLogDetail,
  ingestedAt: string,
): ChatMessageRecord[] {
  const chat = detail.chat;
  if (!chat || chat.length === 0) {
    return [];
  }

  const meta = resolveLogMetadata(summary, detail);
  const records: ChatMessageRecord[] = [];

  for (const [index, entry] of chat.entries()) {
    const message = typeof entry.msg === "string" ? entry.msg.trim() : "";
    if (message.length === 0) {
      continue;
    }

    records.push({
      recordId: `${summary.id}:${index}`,
      logId: summary.id,
      title: meta.title,
      map: meta.map,
      sourceDateEpochSeconds: meta.sourceDateEpochSeconds,
      sourceDateIso: meta.sourceDateIso,
      messageIndex: index,
      steamId: typeof entry.steamid === "string" ? entry.steamid : null,
      playerName: typeof entry.name === "string" ? entry.name : null,
      message,
      messageLower: normaliseMessage(message),
      ingestedAt,
    });
  }

  return records;
}

function classesPlayedCsv(player: LogsTfPlayerDetail): string | null {
  if (!player.class_stats || player.class_stats.length === 0) {
    return null;
  }

  const classNames = new Set<string>();

  for (const classStat of player.class_stats) {
    if (typeof classStat.type === "string" && classStat.type.length > 0) {
      classNames.add(classStat.type);
    }
  }

  return classNames.size === 0 ? null : [...classNames].join(",");
}

export function extractPlayerSummaryRecords(
  summary: LogsTfLogSummary,
  detail: LogsTfLogDetail,
  ingestedAt: string,
): PlayerSummaryRecord[] {
  const players = detail.players;
  if (!players) {
    return [];
  }

  const meta = resolveLogMetadata(summary, detail);
  const records: PlayerSummaryRecord[] = [];

  for (const [steamId, player] of Object.entries(players)) {
    records.push({
      recordId: `${summary.id}:${steamId}`,
      logId: summary.id,
      title: meta.title,
      map: meta.map,
      sourceDateEpochSeconds: meta.sourceDateEpochSeconds,
      sourceDateIso: meta.sourceDateIso,
      steamId,
      team: player.team ?? null,
      kills: toNullableNumber(player.kills),
      assists: toNullableNumber(player.assists),
      deaths: toNullableNumber(player.deaths),
      damageDealt: toNullableNumber(player.dmg),
      damageTaken: toNullableNumber(player.dt),
      healingDone: toNullableNumber(player.heal),
      ubersUsed: toNullableNumber(player.ubers),
      classesPlayedCsv: classesPlayedCsv(player),
      ingestedAt,
    });
  }

  return records;
}

export function sortSummariesAscending(summaries: readonly LogsTfLogSummary[]): LogsTfLogSummary[] {
  return [...summaries].sort((left, right) => left.id - right.id);
}

export function dedupeSummariesByLogId(summaries: readonly LogsTfLogSummary[]): LogsTfLogSummary[] {
  const deduped = new Map<number, LogsTfLogSummary>();

  for (const summary of summaries) {
    deduped.set(summary.id, summary);
  }

  return [...deduped.values()];
}

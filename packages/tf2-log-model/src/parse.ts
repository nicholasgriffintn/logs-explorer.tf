import type {
  LogsTfChatMessage,
  LogsTfListResponse,
  LogsTfLogDetail,
  LogsTfLogInfo,
  LogsTfPlayerClassStats,
  LogsTfPlayerDetail,
  LogsTfLogSummary,
  LogsTfTeamDetail,
  LogsTfUploaderInfo,
} from "./types";

type JsonObject = Record<string, unknown>;

function isObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null;
}

function asNumber(value: unknown, path: string): number {
  if (typeof value !== "number" || Number.isNaN(value)) {
    throw new Error(`Expected number at ${path}`);
  }

  return value;
}

function asString(value: unknown, path: string): string {
  if (typeof value !== "string") {
    throw new Error(`Expected string at ${path}`);
  }

  return value;
}

function asBoolean(value: unknown, path: string): boolean {
  if (typeof value !== "boolean") {
    throw new Error(`Expected boolean at ${path}`);
  }

  return value;
}

function optionalNumber(value: unknown): number | undefined {
  return typeof value === "number" && !Number.isNaN(value) ? value : undefined;
}

function optionalString(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

function parseUploader(value: unknown): number | string | LogsTfUploaderInfo | undefined {
  if (typeof value === "number" || typeof value === "string") {
    return value;
  }

  if (!isObject(value)) {
    return undefined;
  }

  const parsed: LogsTfUploaderInfo = {
    id: optionalString(value.id) ?? (typeof value.id === "number" ? String(value.id) : undefined),
    name: optionalString(value.name),
    info: optionalString(value.info),
  };

  return parsed;
}

export function parseLogSummary(value: unknown, path = "logs[]"): LogsTfLogSummary {
  if (!isObject(value)) {
    throw new Error(`Expected object at ${path}`);
  }

  return {
    id: asNumber(value.id, `${path}.id`),
    title: asString(value.title, `${path}.title`),
    map: asString(value.map, `${path}.map`),
    date: asNumber(value.date, `${path}.date`),
    views: asNumber(value.views, `${path}.views`),
    players: asNumber(value.players, `${path}.players`),
  };
}

export function parseLogListResponse(value: unknown): LogsTfListResponse {
  if (!isObject(value)) {
    throw new Error("Expected object at root");
  }

  const logs = value.logs;
  if (!Array.isArray(logs)) {
    throw new Error("Expected array at logs");
  }

  const parameters = value.parameters;

  return {
    success: asBoolean(value.success, "success"),
    results: asNumber(value.results, "results"),
    total: asNumber(value.total, "total"),
    parameters: isObject(parameters) ? parameters : {},
    logs: logs.map((entry, index) => parseLogSummary(entry, `logs[${index}]`)),
  };
}

function parseTeams(value: unknown): Record<string, LogsTfTeamDetail> | undefined {
  if (!isObject(value)) {
    return undefined;
  }

  const teams: Record<string, LogsTfTeamDetail> = {};

  for (const [teamName, teamValue] of Object.entries(value)) {
    if (!isObject(teamValue)) {
      continue;
    }

    teams[teamName] = {
      score: optionalNumber(teamValue.score),
      kills: optionalNumber(teamValue.kills),
      deaths: optionalNumber(teamValue.deaths),
      dmg: optionalNumber(teamValue.dmg),
      charges: optionalNumber(teamValue.charges),
      drops: optionalNumber(teamValue.drops),
      firstcaps: optionalNumber(teamValue.firstcaps),
      caps: optionalNumber(teamValue.caps),
    };
  }

  return teams;
}

function parseInfo(value: unknown): LogsTfLogInfo | undefined {
  if (!isObject(value)) {
    return undefined;
  }

  return {
    map: optionalString(value.map),
    date: optionalNumber(value.date),
    title: optionalString(value.title),
    total_length: optionalNumber(value.total_length),
    uploader: parseUploader(value.uploader),
  };
}

function parseChat(value: unknown): LogsTfChatMessage[] | undefined {
  if (!Array.isArray(value)) {
    return undefined;
  }

  const chat: LogsTfChatMessage[] = [];

  for (const entry of value) {
    if (!isObject(entry)) {
      continue;
    }

    chat.push({
      ...entry,
      steamid: optionalString(entry.steamid),
      name: optionalString(entry.name),
      msg: optionalString(entry.msg),
    });
  }

  return chat;
}

function parsePlayers(value: unknown): Record<string, LogsTfPlayerDetail> | undefined {
  if (!isObject(value)) {
    return undefined;
  }

  const players: Record<string, LogsTfPlayerDetail> = {};

  for (const [steamId, playerValue] of Object.entries(value)) {
    if (!isObject(playerValue)) {
      continue;
    }

    players[steamId] = {
      ...playerValue,
      team: optionalString(playerValue.team),
      kills: optionalNumber(playerValue.kills),
      assists: optionalNumber(playerValue.assists),
      deaths: optionalNumber(playerValue.deaths),
      dmg: optionalNumber(playerValue.dmg),
      dt: optionalNumber(playerValue.dt),
      heal: optionalNumber(playerValue.heal),
      ubers: optionalNumber(playerValue.ubers),
      class_stats: Array.isArray(playerValue.class_stats)
        ? playerValue.class_stats.filter((entry): entry is LogsTfPlayerClassStats =>
            isObject(entry),
          )
        : undefined,
    };
  }

  return players;
}

export function parseLogDetailResponse(value: unknown): LogsTfLogDetail {
  if (!isObject(value)) {
    throw new Error("Expected object at root");
  }

  const success = asBoolean(value.success, "success");
  if (!success) {
    throw new Error("logs.tf returned success=false for log detail request");
  }

  return {
    ...value,
    success,
    version: optionalNumber(value.version),
    length: optionalNumber(value.length),
    info: parseInfo(value.info),
    teams: parseTeams(value.teams),
    players: parsePlayers(value.players),
    chat: parseChat(value.chat),
  };
}

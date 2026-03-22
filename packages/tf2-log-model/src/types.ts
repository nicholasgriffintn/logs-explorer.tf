export interface LogsTfLogSummary {
  id: number;
  title: string;
  map: string;
  date: number;
  views: number;
  players: number;
}

export interface LogsTfListResponse {
  success: boolean;
  results: number;
  total: number;
  parameters: Record<string, unknown>;
  logs: LogsTfLogSummary[];
}

export interface LogsTfTeamDetail {
  score?: number;
  kills?: number;
  deaths?: number;
  dmg?: number;
  charges?: number;
  drops?: number;
  firstcaps?: number;
  caps?: number;
}

export interface LogsTfLogInfo {
  map?: string;
  date?: number;
  title?: string;
  total_length?: number;
  uploader?: number | string | LogsTfUploaderInfo;
}

export interface LogsTfUploaderInfo {
  id?: string;
  name?: string;
  info?: string;
}

export interface LogsTfChatMessage {
  steamid?: string;
  name?: string;
  msg?: string;
  [key: string]: unknown;
}

export interface LogsTfPlayerClassStats {
  type?: string;
  [key: string]: unknown;
}

export interface LogsTfPlayerDetail {
  team?: string;
  kills?: number;
  assists?: number;
  deaths?: number;
  dmg?: number;
  dt?: number;
  heal?: number;
  ubers?: number;
  class_stats?: LogsTfPlayerClassStats[];
  [key: string]: unknown;
}

export interface LogsTfLogDetail {
  success: boolean;
  version?: number;
  length?: number;
  info?: LogsTfLogInfo;
  teams?: Record<string, LogsTfTeamDetail>;
  players?: Record<string, LogsTfPlayerDetail>;
  chat?: LogsTfChatMessage[];
  [key: string]: unknown;
}

export interface NormalizedLogRecord {
  recordId: string;
  logId: number;
  title: string;
  map: string;
  sourceDateEpochSeconds: number;
  sourceDateIso: string;
  sourcePlayerCount: number;
  sourceViewCount: number;
  durationSeconds: number | null;
  redScore: number | null;
  blueScore: number | null;
  uploaderSteamId: string | null;
  payloadSchemaVersion: number | null;
  ingestedAt: string;
}

export interface ChatMessageRecord {
  recordId: string;
  logId: number;
  title: string;
  map: string;
  sourceDateEpochSeconds: number;
  sourceDateIso: string;
  messageIndex: number;
  steamId: string | null;
  playerName: string | null;
  message: string;
  messageLower: string;
  ingestedAt: string;
}

export interface PlayerSummaryRecord {
  recordId: string;
  logId: number;
  title: string;
  map: string;
  sourceDateEpochSeconds: number;
  sourceDateIso: string;
  steamId: string;
  team: string | null;
  kills: number | null;
  assists: number | null;
  deaths: number | null;
  damageDealt: number | null;
  damageTaken: number | null;
  healingDone: number | null;
  ubersUsed: number | null;
  classesPlayedCsv: string | null;
  ingestedAt: string;
}

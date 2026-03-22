import { expect, test } from "vite-plus/test";
import {
  dedupeSummariesByLogId,
  extractChatMessageRecords,
  extractPlayerSummaryRecords,
  normalizeLogRecord,
  parseLogDetailResponse,
  parseLogListResponse,
  sortSummariesAscending,
} from "../src";

test("parseLogListResponse parses expected structure", () => {
  const payload = {
    success: true,
    results: 2,
    total: 100,
    parameters: {},
    logs: [
      {
        id: 10,
        title: "test",
        map: "cp_process",
        date: 1774213787,
        views: 0,
        players: 12,
      },
      {
        id: 11,
        title: "test2",
        map: "cp_gullywash",
        date: 1774213888,
        views: 5,
        players: 13,
      },
    ],
  };

  const parsed = parseLogListResponse(payload);
  expect(parsed.logs).toHaveLength(2);
  expect(parsed.logs[0].id).toBe(10);
});

test("parseLogDetailResponse rejects unsuccessful payload", () => {
  expect(() => parseLogDetailResponse({ success: false })).toThrow(/success=false/);
});

test("normalizeLogRecord creates stable analytics record", () => {
  const summary = {
    id: 4031052,
    title: "serveme.tf #1537392 BLU vs RED",
    map: "cp_snakewater_final1",
    date: 1774213787,
    views: 0,
    players: 13,
  };

  const detail = parseLogDetailResponse({
    success: true,
    version: 3,
    length: 1423,
    info: {
      map: "cp_snakewater_final1",
      date: 1774213787,
      title: "serveme.tf #1537392 BLU vs RED",
      uploader: {
        id: "76561197960497430",
        name: "Arie - VanillaTF2.org",
      },
    },
    teams: {
      Red: { score: 0 },
      Blue: { score: 1 },
    },
  });

  const normalized = normalizeLogRecord(summary, detail, "2026-03-22T12:00:00.000Z");

  expect(normalized.logId).toBe(4031052);
  expect(normalized.durationSeconds).toBe(1423);
  expect(normalized.blueScore).toBe(1);
  expect(normalized.payloadSchemaVersion).toBe(3);
  expect(normalized.uploaderSteamId).toBe("76561197960497430");
});

test("summary helpers dedupe and sort by id", () => {
  const summaries = [
    { id: 3, title: "c", map: "cp", date: 3, views: 1, players: 12 },
    { id: 2, title: "b", map: "cp", date: 2, views: 1, players: 12 },
    { id: 3, title: "c2", map: "cp", date: 3, views: 1, players: 12 },
  ];

  const deduped = dedupeSummariesByLogId(summaries);
  const sorted = sortSummariesAscending(deduped);

  expect(deduped).toHaveLength(2);
  expect(sorted.map((summary) => summary.id)).toEqual([2, 3]);
});

test("extract chat and player records for analytics datasets", () => {
  const summary = {
    id: 4031052,
    title: "serveme.tf #1537392 BLU vs RED",
    map: "cp_snakewater_final1",
    date: 1774213787,
    views: 0,
    players: 13,
  };

  const detail = parseLogDetailResponse({
    success: true,
    info: {
      map: "cp_snakewater_final1",
      date: 1774213787,
      title: "serveme.tf #1537392 BLU vs RED",
    },
    chat: [
      { steamid: "[U:1:1]", name: "Player A", msg: "holy spam" },
      { steamid: "[U:1:2]", name: "Player B", msg: "   " },
      { steamid: "[U:1:3]", name: "Player C", msg: "GG" },
    ],
    players: {
      "[U:1:1]": {
        team: "Red",
        kills: 10,
        assists: 4,
        deaths: 6,
        dmg: 2500,
        dt: 1800,
        heal: 300,
        ubers: 1,
        class_stats: [{ type: "soldier" }, { type: "scout" }],
      },
    },
  });

  const chat = extractChatMessageRecords(summary, detail, "2026-03-22T12:00:00.000Z");
  const players = extractPlayerSummaryRecords(summary, detail, "2026-03-22T12:00:00.000Z");

  expect(chat).toHaveLength(2);
  expect(chat[0].recordId).toBe("4031052:0");
  expect(chat[0].messageLower).toBe("holy spam");
  expect(players).toHaveLength(1);
  expect(players[0].recordId).toBe("4031052:[U:1:1]");
  expect(players[0].classesPlayedCsv).toBe("soldier,scout");
});

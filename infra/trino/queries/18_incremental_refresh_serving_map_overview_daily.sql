-- Incrementally refreshes map/day serving rows.
-- Strategy: overwrite a rolling date window by match_date.

CREATE TABLE IF NOT EXISTS tf2.default.serving_map_overview_daily
WITH (
  format = 'PARQUET',
  partitioning = ARRAY['month(match_date)']
) AS
WITH game_base AS (
  SELECT
    l.logid,
    l.map,
    CAST(from_iso8601_timestamp(l.sourcedateiso) AS DATE) AS match_date,
    COALESCE(l.durationseconds, 0) AS duration_seconds,
    ABS(COALESCE(l.redscore, 0) - COALESCE(l.bluescore, 0)) AS score_delta
  FROM tf2.default.logs l
),
kills_by_game AS (
  SELECT
    logid,
    SUM(COALESCE(kills, 0)) AS total_kills
  FROM tf2.default.summaries
  GROUP BY logid
),
map_games AS (
  SELECT
    gb.map,
    gb.match_date,
    COUNT(*) AS games,
    AVG(gb.duration_seconds) / 60.0 AS avg_duration_minutes,
    AVG(COALESCE(kbg.total_kills, 0)) AS avg_total_kills,
    AVG(COALESCE(kbg.total_kills, 0) / NULLIF(gb.duration_seconds / 60.0, 0)) AS avg_kills_per_minute,
    AVG(CASE WHEN gb.score_delta <= 1 THEN 1.0 ELSE 0.0 END) AS close_game_rate,
    AVG(CASE WHEN gb.score_delta >= 4 THEN 1.0 ELSE 0.0 END) AS blowout_rate
  FROM game_base gb
  LEFT JOIN kills_by_game kbg ON kbg.logid = gb.logid
  GROUP BY gb.map, gb.match_date
),
map_player_features AS (
  SELECT
    map,
    match_date,
    AVG(impact_index) AS avg_player_impact_index,
    AVG(negative_chat_ratio) AS avg_negative_chat_ratio,
    AVG(CAST(possible_tilt_label AS DOUBLE)) AS tilt_signal_rate,
    APPROX_DISTINCT(steamid) AS active_players
  FROM tf2.default.features_player_match
  GROUP BY map, match_date
)
SELECT
  mg.map,
  mg.match_date,
  mg.games,
  ROUND(mg.avg_duration_minutes, 3) AS avg_duration_minutes,
  ROUND(mg.avg_total_kills, 3) AS avg_total_kills,
  ROUND(mg.avg_kills_per_minute, 3) AS avg_kills_per_minute,
  ROUND(mg.close_game_rate, 4) AS close_game_rate,
  ROUND(mg.blowout_rate, 4) AS blowout_rate,
  COALESCE(mpf.active_players, 0) AS active_players,
  ROUND(COALESCE(mpf.avg_player_impact_index, 0.0), 4) AS avg_player_impact_index,
  ROUND(COALESCE(mpf.avg_negative_chat_ratio, 0.0), 4) AS avg_negative_chat_ratio,
  ROUND(COALESCE(mpf.tilt_signal_rate, 0.0), 4) AS tilt_signal_rate,
  CURRENT_TIMESTAMP AS updated_at
FROM map_games mg
LEFT JOIN map_player_features mpf
  ON mpf.map = mg.map
 AND mpf.match_date = mg.match_date;

DELETE FROM tf2.default.serving_map_overview_daily
WHERE match_date >= (
  SELECT COALESCE(
    DATE_ADD('day', -7, MAX(CAST(from_iso8601_timestamp(sourcedateiso) AS DATE))),
    DATE '1970-01-01'
  ) AS refresh_start_date
  FROM tf2.default.logs
);

INSERT INTO tf2.default.serving_map_overview_daily
WITH bounds AS (
  SELECT COALESCE(
    DATE_ADD('day', -7, MAX(CAST(from_iso8601_timestamp(sourcedateiso) AS DATE))),
    DATE '1970-01-01'
  ) AS refresh_start_date
  FROM tf2.default.logs
),
game_base AS (
  SELECT
    l.logid,
    l.map,
    CAST(from_iso8601_timestamp(l.sourcedateiso) AS DATE) AS match_date,
    COALESCE(l.durationseconds, 0) AS duration_seconds,
    ABS(COALESCE(l.redscore, 0) - COALESCE(l.bluescore, 0)) AS score_delta
  FROM tf2.default.logs l
  CROSS JOIN bounds b
  WHERE CAST(from_iso8601_timestamp(l.sourcedateiso) AS DATE) >= b.refresh_start_date
),
kills_by_game AS (
  SELECT
    s.logid,
    SUM(COALESCE(s.kills, 0)) AS total_kills
  FROM tf2.default.summaries s
  JOIN game_base gb ON gb.logid = s.logid
  GROUP BY s.logid
),
map_games AS (
  SELECT
    gb.map,
    gb.match_date,
    COUNT(*) AS games,
    AVG(gb.duration_seconds) / 60.0 AS avg_duration_minutes,
    AVG(COALESCE(kbg.total_kills, 0)) AS avg_total_kills,
    AVG(COALESCE(kbg.total_kills, 0) / NULLIF(gb.duration_seconds / 60.0, 0)) AS avg_kills_per_minute,
    AVG(CASE WHEN gb.score_delta <= 1 THEN 1.0 ELSE 0.0 END) AS close_game_rate,
    AVG(CASE WHEN gb.score_delta >= 4 THEN 1.0 ELSE 0.0 END) AS blowout_rate
  FROM game_base gb
  LEFT JOIN kills_by_game kbg ON kbg.logid = gb.logid
  GROUP BY gb.map, gb.match_date
),
map_player_features AS (
  SELECT
    fpm.map,
    fpm.match_date,
    AVG(fpm.impact_index) AS avg_player_impact_index,
    AVG(fpm.negative_chat_ratio) AS avg_negative_chat_ratio,
    AVG(CAST(fpm.possible_tilt_label AS DOUBLE)) AS tilt_signal_rate,
    APPROX_DISTINCT(fpm.steamid) AS active_players
  FROM tf2.default.features_player_match fpm
  CROSS JOIN bounds b
  WHERE fpm.match_date >= b.refresh_start_date
  GROUP BY fpm.map, fpm.match_date
)
SELECT
  mg.map,
  mg.match_date,
  mg.games,
  ROUND(mg.avg_duration_minutes, 3) AS avg_duration_minutes,
  ROUND(mg.avg_total_kills, 3) AS avg_total_kills,
  ROUND(mg.avg_kills_per_minute, 3) AS avg_kills_per_minute,
  ROUND(mg.close_game_rate, 4) AS close_game_rate,
  ROUND(mg.blowout_rate, 4) AS blowout_rate,
  COALESCE(mpf.active_players, 0) AS active_players,
  ROUND(COALESCE(mpf.avg_player_impact_index, 0.0), 4) AS avg_player_impact_index,
  ROUND(COALESCE(mpf.avg_negative_chat_ratio, 0.0), 4) AS avg_negative_chat_ratio,
  ROUND(COALESCE(mpf.tilt_signal_rate, 0.0), 4) AS tilt_signal_rate,
  CURRENT_TIMESTAMP AS updated_at
FROM map_games mg
LEFT JOIN map_player_features mpf
  ON mpf.map = mg.map
 AND mpf.match_date = mg.match_date;

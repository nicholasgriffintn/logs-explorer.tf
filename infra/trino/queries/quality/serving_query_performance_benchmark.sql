-- Benchmark pack for serving query latency checks.
-- Run with:
--   docker exec -i tf2-trino trino < infra/trino/queries/quality/serving_query_performance_benchmark.sql
--
-- Use EXPLAIN ANALYZE output to record wall time and scanned rows.
-- Repeat each query at least 5 times and record P95.

-- Benchmark 1: player profile point lookup with percentile context.
EXPLAIN ANALYZE
WITH params AS (
  SELECT CAST('76561197960435530' AS VARCHAR) AS target_steamid
),
target AS (
  SELECT
    spp.steamid,
    spp.career_avg_impact,
    spp.career_win_rate,
    spp.tilt_risk_rate
  FROM tf2.default.serving_player_profiles spp
  JOIN params p
    ON p.target_steamid = spp.steamid
),
population AS (
  SELECT
    COUNT(*) AS players,
    SUM(CASE WHEN spp.career_avg_impact < t.career_avg_impact THEN 1 ELSE 0 END) AS impact_rank_lt,
    SUM(CASE WHEN spp.career_win_rate < t.career_win_rate THEN 1 ELSE 0 END) AS win_rate_rank_lt,
    SUM(CASE WHEN spp.tilt_risk_rate < t.tilt_risk_rate THEN 1 ELSE 0 END) AS tilt_risk_rank_lt
  FROM tf2.default.serving_player_profiles spp
  CROSS JOIN target t
)
SELECT
  t.steamid,
  CASE
    WHEN p.players <= 1 THEN 0.0
    ELSE 100.0 * CAST(p.impact_rank_lt AS DOUBLE) / (CAST(p.players AS DOUBLE) - 1.0)
  END AS impact_percentile,
  CASE
    WHEN p.players <= 1 THEN 0.0
    ELSE 100.0 * CAST(p.win_rate_rank_lt AS DOUBLE) / (CAST(p.players AS DOUBLE) - 1.0)
  END AS win_rate_percentile,
  CASE
    WHEN p.players <= 1 THEN 0.0
    ELSE 100.0 * CAST(p.tilt_risk_rank_lt AS DOUBLE) / (CAST(p.players AS DOUBLE) - 1.0)
  END AS tilt_risk_percentile
FROM target t
CROSS JOIN population p;

-- Benchmark 2: map competitiveness rollup for recent dashboard window.
EXPLAIN ANALYZE
WITH params AS (
  SELECT
    DATE_ADD('day', -30, CURRENT_DATE) AS start_date,
    CAST(10 AS BIGINT) AS min_total_games
),
recent AS (
  SELECT
    map,
    games,
    active_players,
    avg_duration_minutes,
    avg_total_kills,
    avg_kills_per_minute,
    close_game_rate,
    blowout_rate,
    avg_player_impact_index,
    avg_negative_chat_ratio,
    tilt_signal_rate
  FROM tf2.default.serving_map_overview_daily smod
  CROSS JOIN params p
  WHERE smod.match_date >= p.start_date
),
map_rollup AS (
  SELECT
    map,
    SUM(games) AS total_games,
    SUM(active_players) AS total_active_players,
    AVG(avg_duration_minutes) AS avg_duration_minutes,
    AVG(avg_total_kills) AS avg_total_kills,
    AVG(avg_kills_per_minute) AS avg_kills_per_minute,
    SUM(close_game_rate * games) / NULLIF(SUM(games), 0) AS close_game_rate,
    SUM(blowout_rate * games) / NULLIF(SUM(games), 0) AS blowout_rate,
    AVG(avg_player_impact_index) AS avg_player_impact_index,
    AVG(avg_negative_chat_ratio) AS avg_negative_chat_ratio,
    AVG(tilt_signal_rate) AS tilt_signal_rate
  FROM recent
  GROUP BY map
)
SELECT
  map,
  total_games,
  total_active_players,
  close_game_rate - blowout_rate AS competitiveness_index,
  avg_kills_per_minute,
  avg_player_impact_index,
  avg_negative_chat_ratio,
  tilt_signal_rate
FROM map_rollup mr
CROSS JOIN params p
WHERE mr.total_games >= p.min_total_games
ORDER BY competitiveness_index DESC, avg_kills_per_minute DESC, total_games DESC;

-- Benchmark 3: 14-day tilt hotspot rollup.
EXPLAIN ANALYZE
WITH params AS (
  SELECT
    DATE_ADD('day', -14, CURRENT_DATE) AS start_date,
    CAST(10 AS BIGINT) AS min_total_games
),
recent AS (
  SELECT
    map,
    games,
    avg_negative_chat_ratio,
    tilt_signal_rate
  FROM tf2.default.serving_map_overview_daily smod
  CROSS JOIN params p
  WHERE smod.match_date >= p.start_date
)
SELECT
  map,
  SUM(games) AS total_games,
  AVG(avg_negative_chat_ratio) AS avg_negative_chat_ratio,
  AVG(tilt_signal_rate) AS avg_tilt_signal_rate
FROM recent
GROUP BY map
HAVING SUM(games) >= (SELECT min_total_games FROM params)
ORDER BY avg_tilt_signal_rate DESC, avg_negative_chat_ratio DESC, total_games DESC;

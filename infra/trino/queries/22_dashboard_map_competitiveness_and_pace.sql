-- Dashboard pack: map competitiveness and pace.
-- Contract dependency: tf2.default.serving_map_overview_daily only.

WITH params AS (
  SELECT DATE_ADD('day', -30, CURRENT_DATE) AS start_date
),
recent AS (
  SELECT
    map,
    match_date,
    games,
    avg_duration_minutes,
    avg_total_kills,
    avg_kills_per_minute,
    close_game_rate,
    blowout_rate,
    active_players,
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
    COUNT(*) AS days_observed,
    SUM(games) AS total_games,
    SUM(active_players) AS total_active_players,
    AVG(avg_duration_minutes) AS avg_duration_minutes,
    AVG(avg_total_kills) AS avg_total_kills,
    AVG(avg_kills_per_minute) AS avg_kills_per_minute,
    AVG(close_game_rate) AS close_game_rate,
    AVG(blowout_rate) AS blowout_rate,
    AVG(avg_player_impact_index) AS avg_player_impact_index,
    AVG(avg_negative_chat_ratio) AS avg_negative_chat_ratio,
    AVG(tilt_signal_rate) AS tilt_signal_rate
  FROM recent
  GROUP BY map
)
SELECT
  map,
  days_observed,
  total_games,
  total_active_players,
  ROUND(avg_duration_minutes, 3) AS avg_duration_minutes,
  ROUND(avg_total_kills, 3) AS avg_total_kills,
  ROUND(avg_kills_per_minute, 3) AS avg_kills_per_minute,
  ROUND(close_game_rate, 4) AS close_game_rate,
  ROUND(blowout_rate, 4) AS blowout_rate,
  ROUND(close_game_rate - blowout_rate, 4) AS competitiveness_index,
  ROUND(avg_player_impact_index, 4) AS avg_player_impact_index,
  ROUND(avg_negative_chat_ratio, 4) AS avg_negative_chat_ratio,
  ROUND(tilt_signal_rate, 4) AS tilt_signal_rate
FROM map_rollup
WHERE total_games >= 10
ORDER BY competitiveness_index DESC, avg_kills_per_minute DESC, total_games DESC;

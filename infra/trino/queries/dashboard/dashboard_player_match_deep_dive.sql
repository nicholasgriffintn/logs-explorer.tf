-- Dashboard pack: player match deep dive.
-- Contract dependency: tf2.default.serving_player_match_deep_dive only.

WITH params AS (
  -- Set target_steamid to focus one player. Leave NULL to use most recently seen player.
  SELECT
    CAST(NULL AS VARCHAR) AS target_steamid,
    DATE_ADD('day', -60, CURRENT_DATE) AS start_date
),
resolved_params AS (
  SELECT
    COALESCE(
      p.target_steamid,
      (
        SELECT steamid
        FROM tf2.default.serving_player_profiles
        ORDER BY last_seen_at DESC
        LIMIT 1
      )
    ) AS target_steamid,
    p.start_date
  FROM params p
),
recent_player_matches AS (
  SELECT
    spmdd.steamid,
    spmdd.map,
    spmdd.match_date,
    spmdd.won_game,
    spmdd.kills,
    spmdd.damage_dealt,
    spmdd.impact_index,
    spmdd.kda_ratio,
    spmdd.negative_chat_ratio,
    spmdd.behaviour_risk_tier,
    spmdd.impact_tier,
    spmdd.momentum_label
  FROM tf2.default.serving_player_match_deep_dive spmdd
  CROSS JOIN resolved_params p
  WHERE spmdd.steamid = p.target_steamid
    AND spmdd.match_date >= p.start_date
),
map_rollup AS (
  SELECT
    map,
    COUNT(*) AS games,
    ROUND(AVG(CAST(won_game AS DOUBLE)), 4) AS win_rate,
    ROUND(AVG(kills), 3) AS avg_kills,
    ROUND(AVG(damage_dealt), 3) AS avg_damage,
    ROUND(AVG(impact_index), 4) AS avg_impact_index,
    ROUND(AVG(kda_ratio), 3) AS avg_kda_ratio,
    ROUND(AVG(negative_chat_ratio), 4) AS avg_negative_chat_ratio,
    SUM(CASE WHEN behaviour_risk_tier = 'high' THEN 1 ELSE 0 END) AS high_risk_games,
    SUM(CASE WHEN impact_tier IN ('elite', 'strong') THEN 1 ELSE 0 END) AS strong_impact_games,
    SUM(CASE WHEN momentum_label = 'hot' THEN 1 ELSE 0 END) AS hot_momentum_games
  FROM recent_player_matches
  GROUP BY map
)
SELECT
  map,
  games,
  win_rate,
  avg_kills,
  avg_damage,
  avg_impact_index,
  avg_kda_ratio,
  avg_negative_chat_ratio,
  ROUND(CAST(high_risk_games AS DOUBLE) / NULLIF(games, 0), 4) AS high_risk_game_rate,
  ROUND(CAST(strong_impact_games AS DOUBLE) / NULLIF(games, 0), 4) AS strong_impact_game_rate,
  ROUND(CAST(hot_momentum_games AS DOUBLE) / NULLIF(games, 0), 4) AS hot_momentum_game_rate
FROM map_rollup
WHERE games >= 5
ORDER BY avg_impact_index DESC, win_rate DESC, games DESC;

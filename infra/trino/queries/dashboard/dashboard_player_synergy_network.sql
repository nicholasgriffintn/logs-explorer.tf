-- Dashboard pack: player synergy and co-play network.
-- Contract dependency: tf2.default.serving_player_match_deep_dive.

WITH params AS (
  -- Set target_steamid to focus one player. Leave NULL to use most recently seen player.
  SELECT
    CAST(NULL AS VARCHAR) AS target_steamid,
    DATE_ADD('day', -90, CURRENT_DATE) AS start_date,
    CAST(5 AS BIGINT) AS min_shared_games
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
    p.start_date,
    p.min_shared_games
  FROM params p
),
target_games AS (
  SELECT
    d.logid,
    d.steamid AS target_steamid,
    d.team AS target_team,
    d.won_game,
    d.match_date
  FROM tf2.default.serving_player_match_deep_dive d
  CROSS JOIN resolved_params p
  WHERE d.steamid = p.target_steamid
    AND d.match_date >= p.start_date
),
coplay AS (
  SELECT
    tg.target_steamid,
    other.steamid AS other_steamid,
    tg.logid,
    tg.match_date,
    CASE WHEN other.team = tg.target_team THEN 1 ELSE 0 END AS same_team,
    CASE
      WHEN other.team = tg.target_team AND tg.won_game = 1 THEN 1
      ELSE 0
    END AS won_when_teammates,
    other.impact_index AS other_impact_index,
    other.behaviour_risk_tier AS other_behaviour_risk_tier
  FROM target_games tg
  JOIN tf2.default.serving_player_match_deep_dive other
    ON other.logid = tg.logid
   AND other.steamid <> tg.target_steamid
)
SELECT
  target_steamid,
  other_steamid,
  COUNT(*) AS shared_games,
  SUM(same_team) AS games_as_teammates,
  SUM(CASE WHEN same_team = 0 THEN 1 ELSE 0 END) AS games_as_opponents,
  ROUND(AVG(CAST(same_team AS DOUBLE)), 4) AS same_team_rate,
  ROUND(AVG(CASE WHEN same_team = 1 THEN CAST(won_when_teammates AS DOUBLE) END), 4) AS win_rate_when_teammates,
  ROUND(AVG(other_impact_index), 4) AS avg_other_impact_index,
  ROUND(
    AVG(
      CASE WHEN other_behaviour_risk_tier = 'high' THEN 1.0 ELSE 0.0 END
    ),
    4
  ) AS high_risk_pair_rate
FROM coplay
GROUP BY target_steamid, other_steamid
HAVING COUNT(*) >= (SELECT min_shared_games FROM resolved_params)
ORDER BY shared_games DESC, same_team_rate DESC, win_rate_when_teammates DESC;

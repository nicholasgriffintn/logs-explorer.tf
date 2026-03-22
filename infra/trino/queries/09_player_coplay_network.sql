WITH params AS (
  -- Replace with a real Steam ID from tf2.default.summaries.
  SELECT '76561198000000000' AS target_steam_id
),
target_logs AS (
  SELECT
    steamid AS target_steam_id,
    logid,
    team AS target_team
  FROM tf2.default.summaries
  CROSS JOIN params p
  WHERE steamid = p.target_steam_id
),
coplay AS (
  SELECT
    tl.target_steam_id,
    s.steamid AS other_steam_id,
    tl.logid,
    CASE WHEN s.team = tl.target_team THEN 1 ELSE 0 END AS same_team
  FROM target_logs tl
  JOIN tf2.default.summaries s
    ON s.logid = tl.logid
   AND s.steamid <> tl.target_steam_id
),
coplay_scored AS (
  SELECT
    c.target_steam_id,
    c.other_steam_id,
    c.logid,
    c.same_team,
    CASE
      WHEN c.same_team = 1 AND tl.target_team = 'Red' AND l.redscore > l.bluescore THEN 1
      WHEN c.same_team = 1 AND tl.target_team = 'Blue' AND l.bluescore > l.redscore THEN 1
      ELSE 0
    END AS won_when_teammates
  FROM coplay c
  JOIN target_logs tl ON tl.logid = c.logid
  LEFT JOIN tf2.default.logs l ON l.logid = c.logid
)
SELECT
  target_steam_id,
  other_steam_id,
  COUNT(*) AS shared_games,
  SUM(same_team) AS games_as_teammates,
  SUM(CASE WHEN same_team = 0 THEN 1 ELSE 0 END) AS games_as_opponents,
  ROUND(AVG(CAST(same_team AS DOUBLE)), 3) AS same_team_rate,
  ROUND(
    AVG(CASE WHEN same_team = 1 THEN CAST(won_when_teammates AS DOUBLE) END),
    3
  ) AS win_rate_when_teammates
FROM coplay_scored
GROUP BY target_steam_id, other_steam_id
-- Lower this threshold if your dataset is still small.
HAVING COUNT(*) >= 5
ORDER BY shared_games DESC, same_team_rate DESC
LIMIT 300;

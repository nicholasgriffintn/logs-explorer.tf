WITH params AS (
  SELECT
    '' AS anchor_steam_id,
    20 AS min_games_together
),
pair_games AS (
  SELECT
    LEAST(a.steamid, b.steamid) AS player_a,
    GREATEST(a.steamid, b.steamid) AS player_b,
    a.logid,
    a.team,
    COALESCE(a.kills, 0) + COALESCE(b.kills, 0) AS combined_kills,
    COALESCE(a.assists, 0) + COALESCE(b.assists, 0) AS combined_assists,
    COALESCE(a.deaths, 0) + COALESCE(b.deaths, 0) AS combined_deaths,
    COALESCE(a.damagedealt, 0) + COALESCE(b.damagedealt, 0) AS combined_damage,
    CASE
      WHEN a.team = 'Red' AND l.redscore > l.bluescore THEN 1
      WHEN a.team = 'Blue' AND l.bluescore > l.redscore THEN 1
      ELSE 0
    END AS won_game
  FROM tf2.default.summaries a
  JOIN tf2.default.summaries b
    ON a.logid = b.logid
   AND a.team = b.team
   AND a.steamid < b.steamid
  LEFT JOIN tf2.default.logs l ON l.logid = a.logid
  CROSS JOIN params p
  WHERE a.team IN ('Red', 'Blue')
    AND (
      p.anchor_steam_id = ''
      OR a.steamid = p.anchor_steam_id
      OR b.steamid = p.anchor_steam_id
    )
)
SELECT
  player_a,
  player_b,
  COUNT(*) AS games_together,
  ROUND(AVG(combined_kills), 2) AS avg_combined_kills,
  ROUND(AVG(combined_damage), 2) AS avg_combined_damage,
  ROUND(SUM(combined_kills) / NULLIF(SUM(combined_deaths), 0), 3) AS combined_kd_ratio,
  ROUND(AVG(CAST(won_game AS DOUBLE)), 3) AS win_rate_together,
  ROUND(
    AVG(combined_damage) * AVG(CAST(won_game AS DOUBLE)) / 1000.0,
    4
  ) AS synergy_score
FROM pair_games
GROUP BY player_a, player_b
HAVING COUNT(*) >= (SELECT min_games_together FROM params)
ORDER BY synergy_score DESC, games_together DESC
LIMIT 200;

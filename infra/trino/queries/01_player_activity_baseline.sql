WITH player_games AS (
  SELECT
    s.steamid,
    s.logid,
    s.map,
    from_iso8601_timestamp(s.sourcedateiso) AS match_time,
    COALESCE(s.kills, 0) AS kills,
    COALESCE(s.assists, 0) AS assists,
    COALESCE(s.deaths, 0) AS deaths,
    COALESCE(s.damagedealt, 0) AS damage_dealt,
    COALESCE(s.healingdone, 0) AS healing_done,
    COALESCE(s.ubersused, 0) AS ubers_used,
    l.durationseconds,
    CASE
      WHEN s.team = 'Red' AND l.redscore > l.bluescore THEN 1
      WHEN s.team = 'Blue' AND l.bluescore > l.redscore THEN 1
      ELSE 0
    END AS won_game
  FROM tf2.default.summaries s
  LEFT JOIN tf2.default.logs l ON l.logid = s.logid
)
SELECT
  steamid,
  COUNT(*) AS games_played,
  COUNT(DISTINCT map) AS maps_played,
  COUNT(DISTINCT DATE(match_time)) AS active_days,
  MIN(match_time) AS first_seen_at,
  MAX(match_time) AS last_seen_at,
  SUM(kills) AS total_kills,
  SUM(assists) AS total_assists,
  SUM(deaths) AS total_deaths,
  SUM(damage_dealt) AS total_damage_dealt,
  SUM(healing_done) AS total_healing_done,
  SUM(ubers_used) AS total_ubers_used,
  ROUND(SUM(kills) / NULLIF(SUM(deaths), 0), 3) AS kd_ratio,
  ROUND((SUM(kills) + SUM(assists)) / NULLIF(SUM(deaths), 0), 3) AS kda_ratio,
  ROUND(AVG(kills), 2) AS avg_kills_per_game,
  ROUND(AVG(damage_dealt), 2) AS avg_damage_per_game,
  ROUND(
    SUM(damage_dealt)
    / NULLIF(SUM(COALESCE(durationseconds, 0)) / 60.0, 0),
    2
  ) AS damage_per_minute,
  ROUND(AVG(CAST(won_game AS DOUBLE)), 3) AS win_rate
FROM player_games
GROUP BY steamid
-- Lower this threshold if your dataset is still small.
HAVING COUNT(*) >= 20
ORDER BY games_played DESC, kd_ratio DESC;

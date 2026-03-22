WITH params AS (
  SELECT
    30 AS min_games_overall,
    8 AS min_games_on_map
),
player_game_base AS (
  SELECT
    s.steamid,
    s.map,
    s.logid,
    COALESCE(s.damagedealt, 0) AS damage_dealt,
    COALESCE(s.kills, 0) AS kills,
    COALESCE(s.deaths, 0) AS deaths,
    l.durationseconds
  FROM tf2.default.summaries s
  LEFT JOIN tf2.default.logs l ON l.logid = s.logid
),
player_overall AS (
  SELECT
    steamid,
    COUNT(*) AS games_overall,
    AVG(damage_dealt) AS avg_damage_overall,
    AVG(kills) AS avg_kills_overall,
    SUM(kills) / NULLIF(SUM(deaths), 0) AS kd_overall,
    SUM(damage_dealt) / NULLIF(SUM(COALESCE(durationseconds, 0)) / 60.0, 0) AS dpm_overall
  FROM player_game_base
  GROUP BY steamid
  HAVING COUNT(*) >= (SELECT min_games_overall FROM params)
),
player_map AS (
  SELECT
    steamid,
    map,
    COUNT(*) AS games_on_map,
    AVG(damage_dealt) AS avg_damage_on_map,
    AVG(kills) AS avg_kills_on_map,
    SUM(kills) / NULLIF(SUM(deaths), 0) AS kd_on_map,
    SUM(damage_dealt) / NULLIF(SUM(COALESCE(durationseconds, 0)) / 60.0, 0) AS dpm_on_map
  FROM player_game_base
  GROUP BY steamid, map
  HAVING COUNT(*) >= (SELECT min_games_on_map FROM params)
)
SELECT
  pm.steamid,
  pm.map,
  pm.games_on_map,
  po.games_overall,
  ROUND(pm.avg_damage_on_map, 2) AS avg_damage_on_map,
  ROUND(po.avg_damage_overall, 2) AS avg_damage_overall,
  ROUND(pm.avg_kills_on_map, 2) AS avg_kills_on_map,
  ROUND(po.avg_kills_overall, 2) AS avg_kills_overall,
  ROUND(pm.kd_on_map, 3) AS kd_on_map,
  ROUND(po.kd_overall, 3) AS kd_overall,
  ROUND(pm.dpm_on_map, 2) AS dpm_on_map,
  ROUND(po.dpm_overall, 2) AS dpm_overall,
  ROUND(pm.dpm_on_map - po.dpm_overall, 2) AS dpm_uplift,
  ROUND((pm.kd_on_map - po.kd_overall), 3) AS kd_uplift,
  ROUND(
    (pm.dpm_on_map - po.dpm_overall) * LN(1 + pm.games_on_map),
    3
  ) AS specialist_score
FROM player_map pm
JOIN player_overall po ON po.steamid = pm.steamid
ORDER BY specialist_score DESC
LIMIT 300;

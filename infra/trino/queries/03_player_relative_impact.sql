WITH params AS (
  SELECT '76561198000000000' AS target_steam_id
),
team_totals AS (
  SELECT
    logid,
    team,
    SUM(COALESCE(kills, 0)) AS team_kills,
    SUM(COALESCE(damagedealt, 0)) AS team_damage,
    SUM(COALESCE(healingdone, 0)) AS team_healing
  FROM tf2.default.summaries
  WHERE team IN ('Red', 'Blue')
  GROUP BY logid, team
),
player_games AS (
  SELECT
    s.steamid,
    s.logid,
    s.map,
    s.team,
    from_iso8601_timestamp(s.sourcedateiso) AS match_time,
    COALESCE(s.kills, 0) AS kills,
    COALESCE(s.assists, 0) AS assists,
    COALESCE(s.deaths, 0) AS deaths,
    COALESCE(s.damagedealt, 0) AS damage_dealt,
    COALESCE(s.healingdone, 0) AS healing_done,
    COALESCE(s.ubersused, 0) AS ubers_used,
    tt.team_kills,
    tt.team_damage,
    tt.team_healing
  FROM tf2.default.summaries s
  JOIN team_totals tt
    ON tt.logid = s.logid
   AND tt.team = s.team
  CROSS JOIN params p
  WHERE s.steamid = p.target_steam_id
)
SELECT
  steamid,
  logid,
  map,
  team,
  match_time,
  kills,
  assists,
  deaths,
  damage_dealt,
  healing_done,
  ubers_used,
  ROUND(kills / NULLIF(CAST(team_kills AS DOUBLE), 0), 3) AS kill_share_of_team,
  ROUND(damage_dealt / NULLIF(CAST(team_damage AS DOUBLE), 0), 3) AS damage_share_of_team,
  ROUND(healing_done / NULLIF(CAST(team_healing AS DOUBLE), 0), 3) AS healing_share_of_team,
  ROUND(
    0.45 * (damage_dealt / NULLIF(CAST(team_damage AS DOUBLE), 0))
      + 0.35 * (kills / NULLIF(CAST(team_kills AS DOUBLE), 0))
      + 0.20 * (healing_done / NULLIF(CAST(team_healing AS DOUBLE), 0)),
    4
  ) AS weighted_impact_index
FROM player_games
ORDER BY match_time DESC
LIMIT 200;

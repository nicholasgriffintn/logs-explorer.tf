WITH params AS (
  SELECT 4031015 AS target_log_id
),
team_totals AS (
  SELECT
    logid,
    team,
    SUM(COALESCE(kills, 0)) AS team_kills,
    SUM(COALESCE(damagedealt, 0)) AS team_damage,
    SUM(COALESCE(healingdone, 0)) AS team_healing
  FROM tf2.default.summaries
  GROUP BY logid, team
),
game AS (
  SELECT
    l.logid,
    l.title,
    l.map,
    from_iso8601_timestamp(l.sourcedateiso) AS match_time,
    l.durationseconds,
    l.redscore,
    l.bluescore
  FROM tf2.default.logs l
  CROSS JOIN params p
  WHERE l.logid = p.target_log_id
)
SELECT
  g.logid,
  g.title,
  g.map,
  g.match_time,
  g.durationseconds,
  g.redscore,
  g.bluescore,
  s.steamid,
  s.team,
  COALESCE(s.kills, 0) AS kills,
  COALESCE(s.assists, 0) AS assists,
  COALESCE(s.deaths, 0) AS deaths,
  COALESCE(s.damagedealt, 0) AS damage_dealt,
  COALESCE(s.healingdone, 0) AS healing_done,
  COALESCE(s.ubersused, 0) AS ubers_used,
  s.classesplayedcsv,
  ROUND(COALESCE(s.kills, 0) / NULLIF(CAST(tt.team_kills AS DOUBLE), 0), 3) AS kill_share_of_team,
  ROUND(COALESCE(s.damagedealt, 0) / NULLIF(CAST(tt.team_damage AS DOUBLE), 0), 3) AS damage_share_of_team,
  ROUND(COALESCE(s.healingdone, 0) / NULLIF(CAST(tt.team_healing AS DOUBLE), 0), 3) AS healing_share_of_team
FROM game g
JOIN tf2.default.summaries s ON s.logid = g.logid
LEFT JOIN team_totals tt
  ON tt.logid = s.logid
 AND tt.team = s.team
ORDER BY
  s.team,
  COALESCE(s.damagedealt, 0) DESC,
  COALESCE(s.kills, 0) DESC;

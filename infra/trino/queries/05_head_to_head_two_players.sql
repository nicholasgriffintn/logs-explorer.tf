WITH params AS (
  -- Replace both values with real Steam IDs from tf2.default.summaries.
  SELECT
    '76561198000000001' AS player_1_steam_id,
    '76561198000000002' AS player_2_steam_id
),
p1 AS (
  SELECT
    s.logid,
    s.team,
    COALESCE(s.kills, 0) AS kills,
    COALESCE(s.assists, 0) AS assists,
    COALESCE(s.deaths, 0) AS deaths,
    COALESCE(s.damagedealt, 0) AS damage_dealt
  FROM tf2.default.summaries s
  CROSS JOIN params p
  WHERE s.steamid = p.player_1_steam_id
),
p2 AS (
  SELECT
    s.logid,
    s.team,
    COALESCE(s.kills, 0) AS kills,
    COALESCE(s.assists, 0) AS assists,
    COALESCE(s.deaths, 0) AS deaths,
    COALESCE(s.damagedealt, 0) AS damage_dealt
  FROM tf2.default.summaries s
  CROSS JOIN params p
  WHERE s.steamid = p.player_2_steam_id
),
shared_games AS (
  SELECT
    p1.logid,
    p1.team AS p1_team,
    p2.team AS p2_team,
    p1.kills AS p1_kills,
    p1.assists AS p1_assists,
    p1.deaths AS p1_deaths,
    p1.damage_dealt AS p1_damage,
    p2.kills AS p2_kills,
    p2.assists AS p2_assists,
    p2.deaths AS p2_deaths,
    p2.damage_dealt AS p2_damage,
    l.redscore,
    l.bluescore
  FROM p1
  JOIN p2 ON p1.logid = p2.logid
  LEFT JOIN tf2.default.logs l ON l.logid = p1.logid
)
SELECT
  (SELECT player_1_steam_id FROM params) AS player_1_steam_id,
  (SELECT player_2_steam_id FROM params) AS player_2_steam_id,
  COUNT(*) AS games_in_same_log,
  SUM(CASE WHEN p1_team = p2_team THEN 1 ELSE 0 END) AS same_team_games,
  SUM(CASE WHEN p1_team <> p2_team THEN 1 ELSE 0 END) AS opposing_team_games,
  SUM(
    CASE
      WHEN p1_team = 'Red' AND redscore > bluescore THEN 1
      WHEN p1_team = 'Blue' AND bluescore > redscore THEN 1
      ELSE 0
    END
  ) AS player_1_wins,
  SUM(
    CASE
      WHEN p2_team = 'Red' AND redscore > bluescore THEN 1
      WHEN p2_team = 'Blue' AND bluescore > redscore THEN 1
      ELSE 0
    END
  ) AS player_2_wins,
  ROUND(AVG(p1_kills), 2) AS player_1_avg_kills,
  ROUND(AVG(p2_kills), 2) AS player_2_avg_kills,
  ROUND(AVG(p1_damage), 2) AS player_1_avg_damage,
  ROUND(AVG(p2_damage), 2) AS player_2_avg_damage,
  ROUND(
    SUM(p1_kills + p1_assists) / NULLIF(SUM(p1_deaths), 0),
    3
  ) AS player_1_kda_ratio,
  ROUND(
    SUM(p2_kills + p2_assists) / NULLIF(SUM(p2_deaths), 0),
    3
  ) AS player_2_kda_ratio
FROM shared_games;

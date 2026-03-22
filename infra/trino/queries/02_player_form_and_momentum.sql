-- Rolling form window is currently 12 games (11 preceding + current row).
WITH params AS (
  -- Replace with a real Steam ID from tf2.default.summaries.
  SELECT
    '76561198000000000' AS target_steam_id
),
player_matches AS (
  SELECT
    s.steamid,
    s.logid,
    s.map,
    from_iso8601_timestamp(s.sourcedateiso) AS match_time,
    COALESCE(s.kills, 0) AS kills,
    COALESCE(s.assists, 0) AS assists,
    COALESCE(s.deaths, 0) AS deaths,
    COALESCE(s.damagedealt, 0) AS damage_dealt,
    COALESCE(s.healingdone, 0) AS healing_done
  FROM tf2.default.summaries s
  CROSS JOIN params p
  WHERE s.steamid = p.target_steam_id
),
scored AS (
  SELECT
    steamid,
    logid,
    map,
    match_time,
    kills,
    assists,
    deaths,
    damage_dealt,
    healing_done,
    ROW_NUMBER() OVER (PARTITION BY steamid ORDER BY match_time) AS game_number,
    AVG(kills) OVER (
      PARTITION BY steamid
      ORDER BY match_time
      ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) AS rolling_avg_kills,
    AVG(damage_dealt) OVER (
      PARTITION BY steamid
      ORDER BY match_time
      ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) AS rolling_avg_damage,
    AVG((kills + assists) / NULLIF(CAST(deaths AS DOUBLE), 0)) OVER (
      PARTITION BY steamid
      ORDER BY match_time
      ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) AS rolling_kda_ratio,
    AVG(kills) OVER (PARTITION BY steamid) AS career_avg_kills,
    STDDEV_POP(kills) OVER (PARTITION BY steamid) AS career_std_kills
  FROM player_matches
)
SELECT
  steamid,
  logid,
  map,
  match_time,
  game_number,
  kills,
  assists,
  deaths,
  damage_dealt,
  healing_done,
  ROUND(rolling_avg_kills, 2) AS rolling_avg_kills,
  ROUND(rolling_avg_damage, 2) AS rolling_avg_damage,
  ROUND(rolling_kda_ratio, 3) AS rolling_kda_ratio,
  ROUND(
    (kills - career_avg_kills) / NULLIF(career_std_kills, 0),
    3
  ) AS kills_zscore_vs_career
FROM scored
ORDER BY match_time DESC
LIMIT 200;

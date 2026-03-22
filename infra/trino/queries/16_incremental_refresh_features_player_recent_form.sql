-- Incrementally refreshes rolling player-form features.
-- Strategy: recompute all history for players that changed in a rolling date window.

CREATE TABLE IF NOT EXISTS tf2.default.features_player_recent_form
WITH (
  format = 'PARQUET',
  partitioning = ARRAY['month(match_date)']
) AS
WITH scored AS (
  SELECT
    steamid,
    logid,
    map,
    team,
    match_time,
    match_date,
    kills,
    assists,
    deaths,
    damage_dealt,
    healing_done,
    impact_index,
    negative_chat_ratio,
    won_game,
    ROW_NUMBER() OVER (PARTITION BY steamid ORDER BY match_time) AS games_played_to_date,
    AVG(kills) OVER (
      PARTITION BY steamid
      ORDER BY match_time
      ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) AS rolling_5_avg_kills,
    AVG(damage_dealt) OVER (
      PARTITION BY steamid
      ORDER BY match_time
      ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) AS rolling_10_avg_damage,
    AVG(impact_index) OVER (
      PARTITION BY steamid
      ORDER BY match_time
      ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) AS rolling_10_avg_impact,
    AVG(
      (kills + assists) / NULLIF(CAST(deaths AS DOUBLE), 0)
    ) OVER (
      PARTITION BY steamid
      ORDER BY match_time
      ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) AS rolling_10_kda_ratio,
    AVG(CAST(won_game AS DOUBLE)) OVER (
      PARTITION BY steamid
      ORDER BY match_time
      ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) AS rolling_10_win_rate,
    AVG(negative_chat_ratio) OVER (
      PARTITION BY steamid
      ORDER BY match_time
      ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) AS rolling_10_negative_chat_ratio,
    AVG(kills) OVER (PARTITION BY steamid) AS career_avg_kills,
    AVG(damage_dealt) OVER (PARTITION BY steamid) AS career_avg_damage,
    AVG(impact_index) OVER (PARTITION BY steamid) AS career_avg_impact
  FROM tf2.default.features_player_match
)
SELECT
  steamid,
  logid,
  map,
  team,
  match_time,
  match_date,
  games_played_to_date,
  kills,
  assists,
  deaths,
  damage_dealt,
  healing_done,
  ROUND(impact_index, 4) AS impact_index,
  ROUND(negative_chat_ratio, 4) AS negative_chat_ratio,
  won_game,
  ROUND(rolling_5_avg_kills, 3) AS rolling_5_avg_kills,
  ROUND(rolling_10_avg_damage, 3) AS rolling_10_avg_damage,
  ROUND(rolling_10_avg_impact, 4) AS rolling_10_avg_impact,
  ROUND(rolling_10_kda_ratio, 4) AS rolling_10_kda_ratio,
  ROUND(rolling_10_win_rate, 4) AS rolling_10_win_rate,
  ROUND(rolling_10_negative_chat_ratio, 4) AS rolling_10_negative_chat_ratio,
  ROUND(career_avg_kills, 3) AS career_avg_kills,
  ROUND(career_avg_damage, 3) AS career_avg_damage,
  ROUND(career_avg_impact, 4) AS career_avg_impact,
  ROUND(rolling_5_avg_kills - career_avg_kills, 3) AS form_delta_kills,
  ROUND(rolling_10_avg_damage - career_avg_damage, 3) AS form_delta_damage,
  ROUND(rolling_10_avg_impact - career_avg_impact, 4) AS form_delta_impact,
  CASE
    WHEN (rolling_10_avg_impact - career_avg_impact) >= 0.05 THEN 'hot'
    WHEN (rolling_10_avg_impact - career_avg_impact) <= -0.05 THEN 'cold'
    ELSE 'stable'
  END AS momentum_label
FROM scored;

DELETE FROM tf2.default.features_player_recent_form
WHERE steamid IN (
  SELECT DISTINCT fpm.steamid
  FROM tf2.default.features_player_match fpm
  CROSS JOIN (
    SELECT COALESCE(DATE_ADD('day', -7, MAX(match_date)), DATE '1970-01-01') AS refresh_start_date
    FROM tf2.default.features_player_match
  ) b
  WHERE fpm.match_date >= b.refresh_start_date
);

INSERT INTO tf2.default.features_player_recent_form
WITH bounds AS (
  SELECT COALESCE(DATE_ADD('day', -7, MAX(match_date)), DATE '1970-01-01') AS refresh_start_date
  FROM tf2.default.features_player_match
),
changed_players AS (
  SELECT DISTINCT fpm.steamid
  FROM tf2.default.features_player_match fpm
  CROSS JOIN bounds b
  WHERE fpm.match_date >= b.refresh_start_date
),
scored AS (
  SELECT
    steamid,
    logid,
    map,
    team,
    match_time,
    match_date,
    kills,
    assists,
    deaths,
    damage_dealt,
    healing_done,
    impact_index,
    negative_chat_ratio,
    won_game,
    ROW_NUMBER() OVER (PARTITION BY steamid ORDER BY match_time) AS games_played_to_date,
    AVG(kills) OVER (
      PARTITION BY steamid
      ORDER BY match_time
      ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) AS rolling_5_avg_kills,
    AVG(damage_dealt) OVER (
      PARTITION BY steamid
      ORDER BY match_time
      ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) AS rolling_10_avg_damage,
    AVG(impact_index) OVER (
      PARTITION BY steamid
      ORDER BY match_time
      ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) AS rolling_10_avg_impact,
    AVG(
      (kills + assists) / NULLIF(CAST(deaths AS DOUBLE), 0)
    ) OVER (
      PARTITION BY steamid
      ORDER BY match_time
      ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) AS rolling_10_kda_ratio,
    AVG(CAST(won_game AS DOUBLE)) OVER (
      PARTITION BY steamid
      ORDER BY match_time
      ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) AS rolling_10_win_rate,
    AVG(negative_chat_ratio) OVER (
      PARTITION BY steamid
      ORDER BY match_time
      ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) AS rolling_10_negative_chat_ratio,
    AVG(kills) OVER (PARTITION BY steamid) AS career_avg_kills,
    AVG(damage_dealt) OVER (PARTITION BY steamid) AS career_avg_damage,
    AVG(impact_index) OVER (PARTITION BY steamid) AS career_avg_impact
  FROM tf2.default.features_player_match
  WHERE steamid IN (SELECT steamid FROM changed_players)
)
SELECT
  steamid,
  logid,
  map,
  team,
  match_time,
  match_date,
  games_played_to_date,
  kills,
  assists,
  deaths,
  damage_dealt,
  healing_done,
  ROUND(impact_index, 4) AS impact_index,
  ROUND(negative_chat_ratio, 4) AS negative_chat_ratio,
  won_game,
  ROUND(rolling_5_avg_kills, 3) AS rolling_5_avg_kills,
  ROUND(rolling_10_avg_damage, 3) AS rolling_10_avg_damage,
  ROUND(rolling_10_avg_impact, 4) AS rolling_10_avg_impact,
  ROUND(rolling_10_kda_ratio, 4) AS rolling_10_kda_ratio,
  ROUND(rolling_10_win_rate, 4) AS rolling_10_win_rate,
  ROUND(rolling_10_negative_chat_ratio, 4) AS rolling_10_negative_chat_ratio,
  ROUND(career_avg_kills, 3) AS career_avg_kills,
  ROUND(career_avg_damage, 3) AS career_avg_damage,
  ROUND(career_avg_impact, 4) AS career_avg_impact,
  ROUND(rolling_5_avg_kills - career_avg_kills, 3) AS form_delta_kills,
  ROUND(rolling_10_avg_damage - career_avg_damage, 3) AS form_delta_damage,
  ROUND(rolling_10_avg_impact - career_avg_impact, 4) AS form_delta_impact,
  CASE
    WHEN (rolling_10_avg_impact - career_avg_impact) >= 0.05 THEN 'hot'
    WHEN (rolling_10_avg_impact - career_avg_impact) <= -0.05 THEN 'cold'
    ELSE 'stable'
  END AS momentum_label
FROM scored;

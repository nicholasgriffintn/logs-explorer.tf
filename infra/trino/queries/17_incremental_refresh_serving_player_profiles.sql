-- Incrementally refreshes player profile serving rows.
-- Strategy: recompute profiles only for players with recent feature changes.

CREATE TABLE IF NOT EXISTS tf2.default.serving_player_profiles
WITH (
  format = 'PARQUET'
) AS
WITH latest_form AS (
  SELECT
    steamid,
    match_time AS latest_match_time,
    rolling_5_avg_kills,
    rolling_10_avg_damage,
    rolling_10_avg_impact,
    rolling_10_kda_ratio,
    rolling_10_win_rate,
    rolling_10_negative_chat_ratio,
    form_delta_kills,
    form_delta_damage,
    form_delta_impact,
    momentum_label,
    ROW_NUMBER() OVER (PARTITION BY steamid ORDER BY match_time DESC) AS rn
  FROM tf2.default.features_player_recent_form
),
recent_30 AS (
  SELECT
    steamid,
    AVG(kills) AS recent_30_avg_kills,
    AVG(damage_dealt) AS recent_30_avg_damage,
    AVG(impact_index) AS recent_30_avg_impact,
    AVG(CAST(won_game AS DOUBLE)) AS recent_30_win_rate
  FROM (
    SELECT
      steamid,
      kills,
      damage_dealt,
      impact_index,
      won_game,
      ROW_NUMBER() OVER (PARTITION BY steamid ORDER BY match_time DESC) AS rn
    FROM tf2.default.features_player_match
  ) recent
  WHERE rn <= 30
  GROUP BY steamid
)
SELECT
  b.steamid,
  COUNT(*) AS games_played,
  COUNT(DISTINCT b.map) AS maps_played,
  MIN(b.match_time) AS first_seen_at,
  MAX(b.match_time) AS last_seen_at,
  ROUND(AVG(CAST(b.won_game AS DOUBLE)), 4) AS career_win_rate,
  ROUND(SUM(b.kills) / NULLIF(SUM(b.deaths), 0), 4) AS career_kd_ratio,
  ROUND(AVG(b.kda_ratio), 4) AS career_avg_kda_ratio,
  ROUND(AVG(b.kills), 3) AS career_avg_kills,
  ROUND(AVG(b.damage_dealt), 3) AS career_avg_damage,
  ROUND(AVG(b.damage_per_minute), 3) AS career_avg_damage_per_minute,
  ROUND(AVG(b.impact_index), 4) AS career_avg_impact,
  ROUND(AVG(CAST(b.possible_tilt_label AS DOUBLE)), 4) AS tilt_risk_rate,
  lf.latest_match_time,
  ROUND(lf.rolling_5_avg_kills, 3) AS rolling_5_avg_kills,
  ROUND(lf.rolling_10_avg_damage, 3) AS rolling_10_avg_damage,
  ROUND(lf.rolling_10_avg_impact, 4) AS rolling_10_avg_impact,
  ROUND(lf.rolling_10_kda_ratio, 4) AS rolling_10_kda_ratio,
  ROUND(lf.rolling_10_win_rate, 4) AS rolling_10_win_rate,
  ROUND(lf.rolling_10_negative_chat_ratio, 4) AS rolling_10_negative_chat_ratio,
  ROUND(lf.form_delta_kills, 3) AS form_delta_kills,
  ROUND(lf.form_delta_damage, 3) AS form_delta_damage,
  ROUND(lf.form_delta_impact, 4) AS form_delta_impact,
  lf.momentum_label,
  ROUND(r30.recent_30_avg_kills, 3) AS recent_30_avg_kills,
  ROUND(r30.recent_30_avg_damage, 3) AS recent_30_avg_damage,
  ROUND(r30.recent_30_avg_impact, 4) AS recent_30_avg_impact,
  ROUND(r30.recent_30_win_rate, 4) AS recent_30_win_rate,
  CURRENT_TIMESTAMP AS updated_at
FROM tf2.default.features_player_match b
LEFT JOIN latest_form lf
  ON lf.steamid = b.steamid
 AND lf.rn = 1
LEFT JOIN recent_30 r30
  ON r30.steamid = b.steamid
GROUP BY
  b.steamid,
  lf.latest_match_time,
  lf.rolling_5_avg_kills,
  lf.rolling_10_avg_damage,
  lf.rolling_10_avg_impact,
  lf.rolling_10_kda_ratio,
  lf.rolling_10_win_rate,
  lf.rolling_10_negative_chat_ratio,
  lf.form_delta_kills,
  lf.form_delta_damage,
  lf.form_delta_impact,
  lf.momentum_label,
  r30.recent_30_avg_kills,
  r30.recent_30_avg_damage,
  r30.recent_30_avg_impact,
  r30.recent_30_win_rate;

DELETE FROM tf2.default.serving_player_profiles
WHERE steamid IN (
  SELECT DISTINCT fpm.steamid
  FROM tf2.default.features_player_match fpm
  CROSS JOIN (
    SELECT COALESCE(DATE_ADD('day', -7, MAX(match_date)), DATE '1970-01-01') AS refresh_start_date
    FROM tf2.default.features_player_match
  ) b
  WHERE fpm.match_date >= b.refresh_start_date
);

INSERT INTO tf2.default.serving_player_profiles
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
latest_form AS (
  SELECT
    steamid,
    match_time AS latest_match_time,
    rolling_5_avg_kills,
    rolling_10_avg_damage,
    rolling_10_avg_impact,
    rolling_10_kda_ratio,
    rolling_10_win_rate,
    rolling_10_negative_chat_ratio,
    form_delta_kills,
    form_delta_damage,
    form_delta_impact,
    momentum_label,
    ROW_NUMBER() OVER (PARTITION BY steamid ORDER BY match_time DESC) AS rn
  FROM tf2.default.features_player_recent_form
  WHERE steamid IN (SELECT steamid FROM changed_players)
),
recent_30 AS (
  SELECT
    steamid,
    AVG(kills) AS recent_30_avg_kills,
    AVG(damage_dealt) AS recent_30_avg_damage,
    AVG(impact_index) AS recent_30_avg_impact,
    AVG(CAST(won_game AS DOUBLE)) AS recent_30_win_rate
  FROM (
    SELECT
      steamid,
      kills,
      damage_dealt,
      impact_index,
      won_game,
      ROW_NUMBER() OVER (PARTITION BY steamid ORDER BY match_time DESC) AS rn
    FROM tf2.default.features_player_match
    WHERE steamid IN (SELECT steamid FROM changed_players)
  ) recent
  WHERE rn <= 30
  GROUP BY steamid
)
SELECT
  b.steamid,
  COUNT(*) AS games_played,
  COUNT(DISTINCT b.map) AS maps_played,
  MIN(b.match_time) AS first_seen_at,
  MAX(b.match_time) AS last_seen_at,
  ROUND(AVG(CAST(b.won_game AS DOUBLE)), 4) AS career_win_rate,
  ROUND(SUM(b.kills) / NULLIF(SUM(b.deaths), 0), 4) AS career_kd_ratio,
  ROUND(AVG(b.kda_ratio), 4) AS career_avg_kda_ratio,
  ROUND(AVG(b.kills), 3) AS career_avg_kills,
  ROUND(AVG(b.damage_dealt), 3) AS career_avg_damage,
  ROUND(AVG(b.damage_per_minute), 3) AS career_avg_damage_per_minute,
  ROUND(AVG(b.impact_index), 4) AS career_avg_impact,
  ROUND(AVG(CAST(b.possible_tilt_label AS DOUBLE)), 4) AS tilt_risk_rate,
  lf.latest_match_time,
  ROUND(lf.rolling_5_avg_kills, 3) AS rolling_5_avg_kills,
  ROUND(lf.rolling_10_avg_damage, 3) AS rolling_10_avg_damage,
  ROUND(lf.rolling_10_avg_impact, 4) AS rolling_10_avg_impact,
  ROUND(lf.rolling_10_kda_ratio, 4) AS rolling_10_kda_ratio,
  ROUND(lf.rolling_10_win_rate, 4) AS rolling_10_win_rate,
  ROUND(lf.rolling_10_negative_chat_ratio, 4) AS rolling_10_negative_chat_ratio,
  ROUND(lf.form_delta_kills, 3) AS form_delta_kills,
  ROUND(lf.form_delta_damage, 3) AS form_delta_damage,
  ROUND(lf.form_delta_impact, 4) AS form_delta_impact,
  lf.momentum_label,
  ROUND(r30.recent_30_avg_kills, 3) AS recent_30_avg_kills,
  ROUND(r30.recent_30_avg_damage, 3) AS recent_30_avg_damage,
  ROUND(r30.recent_30_avg_impact, 4) AS recent_30_avg_impact,
  ROUND(r30.recent_30_win_rate, 4) AS recent_30_win_rate,
  CURRENT_TIMESTAMP AS updated_at
FROM tf2.default.features_player_match b
JOIN changed_players cp ON cp.steamid = b.steamid
LEFT JOIN latest_form lf
  ON lf.steamid = b.steamid
 AND lf.rn = 1
LEFT JOIN recent_30 r30
  ON r30.steamid = b.steamid
GROUP BY
  b.steamid,
  lf.latest_match_time,
  lf.rolling_5_avg_kills,
  lf.rolling_10_avg_damage,
  lf.rolling_10_avg_impact,
  lf.rolling_10_kda_ratio,
  lf.rolling_10_win_rate,
  lf.rolling_10_negative_chat_ratio,
  lf.form_delta_kills,
  lf.form_delta_damage,
  lf.form_delta_impact,
  lf.momentum_label,
  r30.recent_30_avg_kills,
  r30.recent_30_avg_damage,
  r30.recent_30_avg_impact,
  r30.recent_30_win_rate;

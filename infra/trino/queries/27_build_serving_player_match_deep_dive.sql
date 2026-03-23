-- Rebuilds match-level serving table for deep player analysis dashboards.
DROP TABLE IF EXISTS tf2.default.serving_player_match_deep_dive;

CREATE TABLE tf2.default.serving_player_match_deep_dive
WITH (
  format = 'PARQUET',
  partitioning = ARRAY['month(match_date)']
) AS
WITH deduped_match AS (
  SELECT
    logid,
    steamid,
    match_time,
    match_date,
    map,
    team,
    won_game,
    duration_seconds,
    kills,
    assists,
    deaths,
    damage_dealt,
    healing_done,
    ubers_used,
    classes_played_count,
    kill_share_of_team,
    damage_share_of_team,
    healing_share_of_team,
    impact_index,
    damage_per_minute,
    kda_ratio,
    chat_messages,
    avg_message_length,
    all_caps_messages,
    intense_punctuation_messages,
    negative_lexicon_hits,
    negative_chat_ratio,
    possible_tilt_label
  FROM (
    SELECT
      fpm.*,
      ROW_NUMBER() OVER (PARTITION BY fpm.logid, fpm.steamid ORDER BY fpm.match_time DESC) AS rn
    FROM tf2.default.features_player_match fpm
  ) ranked
  WHERE rn = 1
),
deduped_form AS (
  SELECT
    logid,
    steamid,
    games_played_to_date,
    rolling_5_avg_kills,
    rolling_10_avg_damage,
    rolling_10_avg_impact,
    rolling_10_kda_ratio,
    rolling_10_win_rate,
    rolling_10_negative_chat_ratio,
    form_delta_kills,
    form_delta_damage,
    form_delta_impact,
    momentum_label
  FROM (
    SELECT
      frf.*,
      ROW_NUMBER() OVER (PARTITION BY frf.logid, frf.steamid ORDER BY frf.match_time DESC) AS rn
    FROM tf2.default.features_player_recent_form frf
  ) ranked
  WHERE rn = 1
)
SELECT
  fpm.logid,
  fpm.steamid,
  fpm.match_time,
  fpm.match_date,
  fpm.map,
  fpm.team,
  fpm.won_game,
  fpm.duration_seconds,
  fpm.kills,
  fpm.assists,
  fpm.deaths,
  fpm.damage_dealt,
  fpm.healing_done,
  fpm.ubers_used,
  fpm.classes_played_count,
  ROUND(fpm.kill_share_of_team, 4) AS kill_share_of_team,
  ROUND(fpm.damage_share_of_team, 4) AS damage_share_of_team,
  ROUND(fpm.healing_share_of_team, 4) AS healing_share_of_team,
  ROUND(fpm.impact_index, 4) AS impact_index,
  ROUND(fpm.damage_per_minute, 3) AS damage_per_minute,
  ROUND(fpm.kda_ratio, 3) AS kda_ratio,
  fpm.chat_messages,
  ROUND(fpm.avg_message_length, 3) AS avg_message_length,
  fpm.all_caps_messages,
  fpm.intense_punctuation_messages,
  fpm.negative_lexicon_hits,
  ROUND(fpm.negative_chat_ratio, 4) AS negative_chat_ratio,
  fpm.possible_tilt_label,
  frf.games_played_to_date,
  ROUND(frf.rolling_5_avg_kills, 3) AS rolling_5_avg_kills,
  ROUND(frf.rolling_10_avg_damage, 3) AS rolling_10_avg_damage,
  ROUND(frf.rolling_10_avg_impact, 4) AS rolling_10_avg_impact,
  ROUND(frf.rolling_10_kda_ratio, 4) AS rolling_10_kda_ratio,
  ROUND(frf.rolling_10_win_rate, 4) AS rolling_10_win_rate,
  ROUND(frf.rolling_10_negative_chat_ratio, 4) AS rolling_10_negative_chat_ratio,
  ROUND(frf.form_delta_kills, 3) AS form_delta_kills,
  ROUND(frf.form_delta_damage, 3) AS form_delta_damage,
  ROUND(frf.form_delta_impact, 4) AS form_delta_impact,
  COALESCE(frf.momentum_label, 'unknown') AS momentum_label,
  CASE
    WHEN COALESCE(fpm.possible_tilt_label, 0) = 1 OR COALESCE(fpm.negative_chat_ratio, 0.0) >= 0.1800 THEN 'high'
    WHEN COALESCE(fpm.negative_chat_ratio, 0.0) >= 0.0800 THEN 'medium'
    ELSE 'low'
  END AS behaviour_risk_tier,
  CASE
    WHEN COALESCE(fpm.impact_index, 0.0) >= 0.3000 THEN 'elite'
    WHEN COALESCE(fpm.impact_index, 0.0) >= 0.2200 THEN 'strong'
    WHEN COALESCE(fpm.impact_index, 0.0) >= 0.1600 THEN 'average'
    ELSE 'struggling'
  END AS impact_tier,
  CURRENT_TIMESTAMP AS updated_at
FROM deduped_match fpm
LEFT JOIN deduped_form frf
  ON frf.logid = fpm.logid
 AND frf.steamid = fpm.steamid;

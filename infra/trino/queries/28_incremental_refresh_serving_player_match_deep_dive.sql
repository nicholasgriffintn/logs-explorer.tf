-- Incrementally refreshes match-level serving rows for deep player analysis.
-- Strategy: overwrite a rolling 7-day window by match_date.

CREATE TABLE IF NOT EXISTS tf2.default.serving_player_match_deep_dive
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

DELETE FROM tf2.default.serving_player_match_deep_dive
WHERE match_date >= (
  SELECT COALESCE(DATE_ADD('day', -7, MAX(match_date)), DATE '1970-01-01')
  FROM tf2.default.features_player_match
);

INSERT INTO tf2.default.serving_player_match_deep_dive
WITH bounds AS (
  SELECT COALESCE(DATE_ADD('day', -7, MAX(match_date)), DATE '1970-01-01') AS refresh_start_date
  FROM tf2.default.features_player_match
),
recent_base AS (
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
    fpm.kill_share_of_team,
    fpm.damage_share_of_team,
    fpm.healing_share_of_team,
    fpm.impact_index,
    fpm.damage_per_minute,
    fpm.kda_ratio,
    fpm.chat_messages,
    fpm.avg_message_length,
    fpm.all_caps_messages,
    fpm.intense_punctuation_messages,
    fpm.negative_lexicon_hits,
    fpm.negative_chat_ratio,
    fpm.possible_tilt_label
  FROM (
    SELECT
      fpm.*,
      ROW_NUMBER() OVER (PARTITION BY fpm.logid, fpm.steamid ORDER BY fpm.match_time DESC) AS rn
    FROM tf2.default.features_player_match fpm
  ) fpm
  CROSS JOIN bounds b
  WHERE fpm.rn = 1
    AND fpm.match_date >= b.refresh_start_date
),
deduped_form AS (
  SELECT
    frf.logid,
    frf.steamid,
    frf.games_played_to_date,
    frf.rolling_5_avg_kills,
    frf.rolling_10_avg_damage,
    frf.rolling_10_avg_impact,
    frf.rolling_10_kda_ratio,
    frf.rolling_10_win_rate,
    frf.rolling_10_negative_chat_ratio,
    frf.form_delta_kills,
    frf.form_delta_damage,
    frf.form_delta_impact,
    frf.momentum_label
  FROM (
    SELECT
      frf.*,
      ROW_NUMBER() OVER (PARTITION BY frf.logid, frf.steamid ORDER BY frf.match_time DESC) AS rn
    FROM tf2.default.features_player_recent_form frf
    JOIN recent_base rb
      ON rb.logid = frf.logid
     AND rb.steamid = frf.steamid
  ) frf
  WHERE frf.rn = 1
)
SELECT
  rb.logid,
  rb.steamid,
  rb.match_time,
  rb.match_date,
  rb.map,
  rb.team,
  rb.won_game,
  rb.duration_seconds,
  rb.kills,
  rb.assists,
  rb.deaths,
  rb.damage_dealt,
  rb.healing_done,
  rb.ubers_used,
  rb.classes_played_count,
  ROUND(rb.kill_share_of_team, 4) AS kill_share_of_team,
  ROUND(rb.damage_share_of_team, 4) AS damage_share_of_team,
  ROUND(rb.healing_share_of_team, 4) AS healing_share_of_team,
  ROUND(rb.impact_index, 4) AS impact_index,
  ROUND(rb.damage_per_minute, 3) AS damage_per_minute,
  ROUND(rb.kda_ratio, 3) AS kda_ratio,
  rb.chat_messages,
  ROUND(rb.avg_message_length, 3) AS avg_message_length,
  rb.all_caps_messages,
  rb.intense_punctuation_messages,
  rb.negative_lexicon_hits,
  ROUND(rb.negative_chat_ratio, 4) AS negative_chat_ratio,
  rb.possible_tilt_label,
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
    WHEN COALESCE(rb.possible_tilt_label, 0) = 1 OR COALESCE(rb.negative_chat_ratio, 0.0) >= 0.1800 THEN 'high'
    WHEN COALESCE(rb.negative_chat_ratio, 0.0) >= 0.0800 THEN 'medium'
    ELSE 'low'
  END AS behaviour_risk_tier,
  CASE
    WHEN COALESCE(rb.impact_index, 0.0) >= 0.3000 THEN 'elite'
    WHEN COALESCE(rb.impact_index, 0.0) >= 0.2200 THEN 'strong'
    WHEN COALESCE(rb.impact_index, 0.0) >= 0.1600 THEN 'average'
    ELSE 'struggling'
  END AS impact_tier,
  CURRENT_TIMESTAMP AS updated_at
FROM recent_base rb
LEFT JOIN deduped_form frf
  ON frf.logid = rb.logid
 AND frf.steamid = rb.steamid;

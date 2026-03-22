-- Dashboard pack: player profile and momentum deep dive.
-- Contract dependency: tf2.default.serving_player_profiles only.

WITH params AS (
  SELECT CAST('76561197960435530' AS VARCHAR) AS target_steamid
),
population AS (
  SELECT
    steamid,
    career_avg_impact,
    career_win_rate,
    tilt_risk_rate
  FROM tf2.default.serving_player_profiles
),
ranked AS (
  SELECT
    steamid,
    PERCENT_RANK() OVER (ORDER BY career_avg_impact) AS impact_percentile,
    PERCENT_RANK() OVER (ORDER BY career_win_rate) AS win_rate_percentile,
    PERCENT_RANK() OVER (ORDER BY tilt_risk_rate) AS tilt_risk_percentile
  FROM population
)
SELECT
  spp.steamid,
  spp.games_played,
  spp.maps_played,
  spp.first_seen_at,
  spp.last_seen_at,
  spp.career_win_rate,
  spp.career_kd_ratio,
  spp.career_avg_damage,
  spp.career_avg_impact,
  spp.rolling_10_avg_impact,
  spp.form_delta_impact,
  spp.momentum_label,
  spp.rolling_10_negative_chat_ratio,
  spp.tilt_risk_rate,
  ROUND(r.impact_percentile * 100.0, 1) AS impact_percentile,
  ROUND(r.win_rate_percentile * 100.0, 1) AS win_rate_percentile,
  ROUND(r.tilt_risk_percentile * 100.0, 1) AS tilt_risk_percentile,
  spp.updated_at
FROM tf2.default.serving_player_profiles spp
JOIN params p
  ON p.target_steamid = spp.steamid
JOIN ranked r
  ON r.steamid = spp.steamid;

-- Dashboard pack: player profile and momentum deep dive.
-- Contract dependency: tf2.default.serving_player_profiles only.

WITH params AS (
  SELECT CAST('76561197960435530' AS VARCHAR) AS target_steamid
),
target AS (
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
    spp.updated_at
  FROM tf2.default.serving_player_profiles spp
  JOIN params p
    ON p.target_steamid = spp.steamid
),
population AS (
  SELECT
    COUNT(*) AS players,
    SUM(CASE WHEN spp.career_avg_impact < t.career_avg_impact THEN 1 ELSE 0 END) AS impact_rank_lt,
    SUM(CASE WHEN spp.career_win_rate < t.career_win_rate THEN 1 ELSE 0 END) AS win_rate_rank_lt,
    SUM(CASE WHEN spp.tilt_risk_rate < t.tilt_risk_rate THEN 1 ELSE 0 END) AS tilt_risk_rank_lt
  FROM tf2.default.serving_player_profiles spp
  CROSS JOIN target t
)
SELECT
  t.steamid,
  t.games_played,
  t.maps_played,
  t.first_seen_at,
  t.last_seen_at,
  t.career_win_rate,
  t.career_kd_ratio,
  t.career_avg_damage,
  t.career_avg_impact,
  t.rolling_10_avg_impact,
  t.form_delta_impact,
  t.momentum_label,
  t.rolling_10_negative_chat_ratio,
  t.tilt_risk_rate,
  ROUND(
    CASE
      WHEN p.players <= 1 THEN 0.0
      ELSE 100.0 * CAST(p.impact_rank_lt AS DOUBLE) / (CAST(p.players AS DOUBLE) - 1.0)
    END,
    1
  ) AS impact_percentile,
  ROUND(
    CASE
      WHEN p.players <= 1 THEN 0.0
      ELSE 100.0 * CAST(p.win_rate_rank_lt AS DOUBLE) / (CAST(p.players AS DOUBLE) - 1.0)
    END,
    1
  ) AS win_rate_percentile,
  ROUND(
    CASE
      WHEN p.players <= 1 THEN 0.0
      ELSE 100.0 * CAST(p.tilt_risk_rank_lt AS DOUBLE) / (CAST(p.players AS DOUBLE) - 1.0)
    END,
    1
  ) AS tilt_risk_percentile,
  t.updated_at
FROM target t
CROSS JOIN population p;

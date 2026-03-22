-- Dashboard pack: chat behaviour and tilt risk summary.
-- Contract dependencies: serving_player_profiles + serving_map_overview_daily.

-- Section 1: player-level risk by momentum cohort.
WITH params AS (
  SELECT
    CAST(0.1200 AS DOUBLE) AS high_tilt_threshold,
    CAST(0.0800 AS DOUBLE) AS high_negative_chat_threshold
),
cohorts AS (
  SELECT
    spp.momentum_label,
    COUNT(*) AS players,
    AVG(spp.tilt_risk_rate) AS avg_tilt_risk_rate,
    AVG(spp.rolling_10_negative_chat_ratio) AS avg_rolling_negative_chat_ratio,
    SUM(
      CASE
        WHEN spp.tilt_risk_rate >= p.high_tilt_threshold
          OR spp.rolling_10_negative_chat_ratio >= p.high_negative_chat_threshold
        THEN 1
        ELSE 0
      END
    ) AS high_risk_players
  FROM tf2.default.serving_player_profiles spp
  CROSS JOIN params p
  GROUP BY spp.momentum_label
)
SELECT
  momentum_label,
  players,
  high_risk_players,
  ROUND(CAST(high_risk_players AS DOUBLE) / NULLIF(players, 0), 4) AS high_risk_player_rate,
  ROUND(avg_tilt_risk_rate, 4) AS avg_tilt_risk_rate,
  ROUND(avg_rolling_negative_chat_ratio, 4) AS avg_rolling_negative_chat_ratio
FROM cohorts
ORDER BY high_risk_player_rate DESC, players DESC;

-- Section 2: map/day hotspots from recent serving windows.
WITH params AS (
  SELECT DATE_ADD('day', -14, CURRENT_DATE) AS start_date
),
recent AS (
  SELECT
    map,
    match_date,
    games,
    avg_negative_chat_ratio,
    tilt_signal_rate
  FROM tf2.default.serving_map_overview_daily smod
  CROSS JOIN params p
  WHERE smod.match_date >= p.start_date
)
SELECT
  map,
  COUNT(*) AS days_observed,
  SUM(games) AS total_games,
  ROUND(AVG(avg_negative_chat_ratio), 4) AS avg_negative_chat_ratio,
  ROUND(MAX(avg_negative_chat_ratio), 4) AS peak_negative_chat_ratio,
  ROUND(AVG(tilt_signal_rate), 4) AS avg_tilt_signal_rate,
  ROUND(MAX(tilt_signal_rate), 4) AS peak_tilt_signal_rate
FROM recent
GROUP BY map
HAVING SUM(games) >= 10
ORDER BY avg_tilt_signal_rate DESC, avg_negative_chat_ratio DESC, total_games DESC;

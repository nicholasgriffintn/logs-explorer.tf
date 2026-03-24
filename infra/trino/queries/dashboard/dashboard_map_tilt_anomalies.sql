-- Dashboard pack: map-level tilt/behaviour anomaly watch.
-- Contract dependency: tf2.default.serving_map_overview_daily.

WITH params AS (
  SELECT
    DATE_ADD('day', -90, CURRENT_DATE) AS history_start_date,
    DATE_ADD('day', -14, CURRENT_DATE) AS recent_start_date,
    CAST(15 AS BIGINT) AS min_recent_games
),
history AS (
  SELECT
    map,
    match_date,
    games,
    avg_negative_chat_ratio,
    tilt_signal_rate
  FROM tf2.default.serving_map_overview_daily d
  CROSS JOIN params p
  WHERE d.match_date >= p.history_start_date
),
baseline AS (
  SELECT
    map,
    AVG(avg_negative_chat_ratio) AS baseline_negative_chat_ratio,
    STDDEV_POP(avg_negative_chat_ratio) AS baseline_negative_chat_std,
    AVG(tilt_signal_rate) AS baseline_tilt_signal_rate,
    STDDEV_POP(tilt_signal_rate) AS baseline_tilt_signal_std
  FROM history
  GROUP BY map
),
recent AS (
  SELECT
    h.map,
    SUM(h.games) AS recent_games,
    AVG(h.avg_negative_chat_ratio) AS recent_negative_chat_ratio,
    AVG(h.tilt_signal_rate) AS recent_tilt_signal_rate
  FROM history h
  CROSS JOIN params p
  WHERE h.match_date >= p.recent_start_date
  GROUP BY h.map
)
SELECT
  r.map,
  r.recent_games,
  ROUND(r.recent_negative_chat_ratio, 4) AS recent_negative_chat_ratio,
  ROUND(r.recent_tilt_signal_rate, 4) AS recent_tilt_signal_rate,
  ROUND(b.baseline_negative_chat_ratio, 4) AS baseline_negative_chat_ratio,
  ROUND(b.baseline_tilt_signal_rate, 4) AS baseline_tilt_signal_rate,
  ROUND(
    (r.recent_negative_chat_ratio - b.baseline_negative_chat_ratio)
      / NULLIF(b.baseline_negative_chat_std, 0),
    4
  ) AS negative_chat_zscore,
  ROUND(
    (r.recent_tilt_signal_rate - b.baseline_tilt_signal_rate)
      / NULLIF(b.baseline_tilt_signal_std, 0),
    4
  ) AS tilt_signal_zscore
FROM recent r
JOIN baseline b
  ON b.map = r.map
WHERE r.recent_games >= (SELECT min_recent_games FROM params)
ORDER BY tilt_signal_zscore DESC, negative_chat_zscore DESC, recent_games DESC;

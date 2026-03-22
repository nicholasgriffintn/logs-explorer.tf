WITH game_kills AS (
  SELECT
    logid,
    SUM(COALESCE(kills, 0)) AS total_kills
  FROM tf2.default.summaries
  GROUP BY logid
),
game_base AS (
  SELECT
    l.logid,
    l.map,
    from_iso8601_timestamp(l.sourcedateiso) AS match_time,
    COALESCE(l.durationseconds, 0) AS duration_seconds,
    COALESCE(l.redscore, 0) AS red_score,
    COALESCE(l.bluescore, 0) AS blue_score,
    ABS(COALESCE(l.redscore, 0) - COALESCE(l.bluescore, 0)) AS score_delta,
    COALESCE(gk.total_kills, 0) AS total_kills
  FROM tf2.default.logs l
  LEFT JOIN game_kills gk ON gk.logid = l.logid
)
SELECT
  map,
  COUNT(*) AS games,
  ROUND(AVG(duration_seconds) / 60.0, 2) AS avg_duration_minutes,
  ROUND(AVG(total_kills), 2) AS avg_total_kills,
  ROUND(AVG(total_kills / NULLIF(duration_seconds / 60.0, 0)), 2) AS avg_kills_per_minute,
  ROUND(AVG(score_delta), 2) AS avg_score_delta,
  ROUND(AVG(CASE WHEN score_delta <= 1 THEN 1.0 ELSE 0.0 END), 3) AS close_game_rate,
  ROUND(AVG(CASE WHEN score_delta >= 4 THEN 1.0 ELSE 0.0 END), 3) AS blowout_rate
FROM game_base
GROUP BY map
HAVING COUNT(*) >= 20
ORDER BY close_game_rate DESC, games DESC;

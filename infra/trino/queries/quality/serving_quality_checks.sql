-- Serving quality gatekeeping checks for core -> features -> serving pipeline.
-- Returns one row per check with explicit PASS/FAIL status.

WITH
core_logs AS (
  SELECT COUNT(*) AS row_count
  FROM tf2.default.logs
),
features_match AS (
  SELECT COUNT(*) AS row_count
  FROM tf2.default.features_player_match
),
features_recent_form AS (
  SELECT COUNT(*) AS row_count
  FROM tf2.default.features_player_recent_form
),
serving_profiles AS (
  SELECT COUNT(*) AS row_count
  FROM tf2.default.serving_player_profiles
),
serving_maps AS (
  SELECT COUNT(*) AS row_count
  FROM tf2.default.serving_map_overview_daily
),
serving_player_match_deep_dive AS (
  SELECT COUNT(*) AS row_count
  FROM tf2.default.serving_player_match_deep_dive
),
features_match_null_keys AS (
  SELECT COUNT(*) AS row_count
  FROM tf2.default.features_player_match
  WHERE steamid IS NULL OR logid IS NULL
),
features_match_duplicate_keys AS (
  SELECT COUNT(*) AS row_count
  FROM (
    SELECT
      logid,
      steamid,
      COUNT(*) AS duplicate_count
    FROM tf2.default.features_player_match
    GROUP BY logid, steamid
    HAVING COUNT(*) > 1
  ) duplicates
),
recent_form_orphans AS (
  SELECT COUNT(*) AS row_count
  FROM tf2.default.features_player_recent_form frf
  LEFT JOIN tf2.default.features_player_match fpm
    ON fpm.logid = frf.logid
   AND fpm.steamid = frf.steamid
  WHERE fpm.logid IS NULL
),
serving_profile_null_keys AS (
  SELECT COUNT(*) AS row_count
  FROM tf2.default.serving_player_profiles
  WHERE steamid IS NULL
),
serving_map_null_keys AS (
  SELECT COUNT(*) AS row_count
  FROM tf2.default.serving_map_overview_daily
  WHERE map IS NULL OR match_date IS NULL
),
serving_player_match_deep_dive_null_keys AS (
  SELECT COUNT(*) AS row_count
  FROM tf2.default.serving_player_match_deep_dive
  WHERE steamid IS NULL OR logid IS NULL OR match_date IS NULL
),
serving_player_match_deep_dive_duplicate_keys AS (
  SELECT COUNT(*) AS row_count
  FROM (
    SELECT
      logid,
      steamid,
      COUNT(*) AS duplicate_count
    FROM tf2.default.serving_player_match_deep_dive
    GROUP BY logid, steamid
    HAVING COUNT(*) > 1
  ) duplicates
),
features_freshness AS (
  SELECT
    MAX(match_date) AS latest_match_date,
    DATE_DIFF('day', MAX(match_date), CURRENT_DATE) AS freshness_days
  FROM tf2.default.features_player_match
),
serving_freshness AS (
  SELECT
    MAX(match_date) AS latest_serving_match_date,
    DATE_DIFF('day', MAX(match_date), CURRENT_DATE) AS freshness_days
  FROM tf2.default.serving_player_match_deep_dive
),
serving_coverage AS (
  SELECT
    CAST((SELECT COUNT(*) FROM tf2.default.features_player_match) AS DOUBLE) AS feature_rows,
    CAST((SELECT COUNT(*) FROM tf2.default.serving_player_match_deep_dive) AS DOUBLE) AS serving_rows
),
tilt_drift AS (
  WITH dated AS (
    SELECT
      match_date,
      CAST(possible_tilt_label AS DOUBLE) AS tilt_label
    FROM tf2.default.features_player_match
  )
  SELECT
    AVG(CASE WHEN match_date >= DATE_ADD('day', -7, CURRENT_DATE) THEN tilt_label END) AS recent_7d_tilt_rate,
    AVG(
      CASE
        WHEN match_date >= DATE_ADD('day', -35, CURRENT_DATE)
         AND match_date < DATE_ADD('day', -7, CURRENT_DATE)
        THEN tilt_label
      END
    ) AS prior_28d_tilt_rate
  FROM dated
),
checks AS (
  SELECT
    'core_logs_non_empty' AS check_name,
    CASE WHEN row_count > 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    CAST(row_count AS DOUBLE) AS metric_value,
    '> 0 rows' AS threshold,
    'core logs table must not be empty' AS details
  FROM core_logs

  UNION ALL

  SELECT
    'features_player_match_non_empty',
    CASE WHEN row_count > 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(row_count AS DOUBLE),
    '> 0 rows',
    'feature table must contain match rows'
  FROM features_match

  UNION ALL

  SELECT
    'features_player_recent_form_non_empty',
    CASE WHEN row_count > 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(row_count AS DOUBLE),
    '> 0 rows',
    'rolling form table must contain rows'
  FROM features_recent_form

  UNION ALL

  SELECT
    'serving_player_profiles_non_empty',
    CASE WHEN row_count > 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(row_count AS DOUBLE),
    '> 0 rows',
    'player profile serving table must contain rows'
  FROM serving_profiles

  UNION ALL

  SELECT
    'serving_map_overview_daily_non_empty',
    CASE WHEN row_count > 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(row_count AS DOUBLE),
    '> 0 rows',
    'map overview serving table must contain rows'
  FROM serving_maps

  UNION ALL

  SELECT
    'serving_player_match_deep_dive_non_empty',
    CASE WHEN row_count > 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(row_count AS DOUBLE),
    '> 0 rows',
    'player match deep-dive serving table must contain rows'
  FROM serving_player_match_deep_dive

  UNION ALL

  SELECT
    'features_player_match_freshness_days',
    CASE WHEN freshness_days <= 2 THEN 'PASS' ELSE 'FAIL' END,
    CAST(freshness_days AS DOUBLE),
    '<= 2 days',
    'features match table should be refreshed recently'
  FROM features_freshness

  UNION ALL

  SELECT
    'serving_player_match_deep_dive_freshness_days',
    CASE WHEN freshness_days <= 2 THEN 'PASS' ELSE 'FAIL' END,
    CAST(freshness_days AS DOUBLE),
    '<= 2 days',
    'serving deep-dive table should be refreshed recently'
  FROM serving_freshness

  UNION ALL

  SELECT
    'serving_vs_features_row_coverage',
    CASE
      WHEN feature_rows = 0 THEN 'FAIL'
      WHEN serving_rows / feature_rows >= 0.98 THEN 'PASS'
      ELSE 'FAIL'
    END,
    CASE
      WHEN feature_rows = 0 THEN 0.0
      ELSE serving_rows / feature_rows
    END,
    '>= 0.98 ratio',
    'serving deep-dive coverage should stay close to feature row count'
  FROM serving_coverage

  UNION ALL

  /* SELECT
    'tilt_label_recent_drift_abs_delta',
    CASE
      WHEN recent_7d_tilt_rate IS NULL OR prior_28d_tilt_rate IS NULL THEN 'FAIL'
      WHEN ABS(recent_7d_tilt_rate - prior_28d_tilt_rate) <= 0.0500 THEN 'PASS'
      ELSE 'FAIL'
    END,
    ABS(recent_7d_tilt_rate - prior_28d_tilt_rate),
    '<= 0.05 absolute delta',
    'tilt signal drift should remain within expected bounds'
  FROM tilt_drift */

  UNION ALL

  SELECT
    'features_player_match_null_keys',
    CASE WHEN row_count = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(row_count AS DOUBLE),
    '= 0 rows',
    'feature rows must have logid and steamid'
  FROM features_match_null_keys

  UNION ALL

  SELECT
    'features_player_match_duplicate_keys',
    CASE WHEN row_count = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(row_count AS DOUBLE),
    '= 0 rows',
    'feature table must have one row per (logid, steamid)'
  FROM features_match_duplicate_keys

  UNION ALL

  SELECT
    'features_recent_form_orphans',
    CASE WHEN row_count = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(row_count AS DOUBLE),
    '= 0 rows',
    'recent-form rows must map to feature rows'
  FROM recent_form_orphans

  UNION ALL

  SELECT
    'serving_player_profiles_null_keys',
    CASE WHEN row_count = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(row_count AS DOUBLE),
    '= 0 rows',
    'serving player rows must have steamid'
  FROM serving_profile_null_keys

  UNION ALL

  SELECT
    'serving_map_overview_daily_null_keys',
    CASE WHEN row_count = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(row_count AS DOUBLE),
    '= 0 rows',
    'serving map rows must have map and match_date'
  FROM serving_map_null_keys

  UNION ALL

  SELECT
    'serving_player_match_deep_dive_null_keys',
    CASE WHEN row_count = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(row_count AS DOUBLE),
    '= 0 rows',
    'deep-dive serving rows must have logid, steamid, and match_date'
  FROM serving_player_match_deep_dive_null_keys

  UNION ALL

  SELECT
    'serving_player_match_deep_dive_duplicate_keys',
    CASE WHEN row_count = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(row_count AS DOUBLE),
    '= 0 rows',
    'deep-dive serving table must have one row per (logid, steamid)'
  FROM serving_player_match_deep_dive_duplicate_keys

)
SELECT
  check_name,
  status,
  metric_value,
  threshold,
  details
FROM checks
ORDER BY check_name;

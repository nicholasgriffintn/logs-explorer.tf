-- Data quality gatekeeping checks for core -> features -> serving pipeline.
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
)
SELECT
  check_name,
  status,
  metric_value,
  threshold,
  details
FROM checks
ORDER BY check_name;

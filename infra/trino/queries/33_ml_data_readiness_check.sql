-- ML data readiness checks for feature completeness and label health.
-- Returns PASS/FAIL rows with thresholds tuned for baseline model training.

WITH
feature_coverage AS (
  SELECT
    COUNT(*) AS rows_total,
    COUNT(DISTINCT steamid) AS players_total,
    COUNT(DISTINCT logid) AS logs_total,
    COUNT(DISTINCT match_date) AS days_total,
    COUNT(DISTINCT map) AS maps_total,
    ROUND(AVG(CASE WHEN impact_index IS NULL THEN 1 ELSE 0 END), 6) AS impact_null_rate,
    ROUND(AVG(CASE WHEN damage_per_minute IS NULL THEN 1 ELSE 0 END), 6) AS dpm_null_rate,
    ROUND(AVG(CASE WHEN kda_ratio IS NULL THEN 1 ELSE 0 END), 6) AS kda_null_rate,
    ROUND(AVG(CASE WHEN kill_share_of_team IS NULL THEN 1 ELSE 0 END), 6) AS kill_share_null_rate,
    ROUND(AVG(CASE WHEN damage_share_of_team IS NULL THEN 1 ELSE 0 END), 6) AS damage_share_null_rate,
    ROUND(AVG(CASE WHEN healing_share_of_team IS NULL THEN 1 ELSE 0 END), 6) AS healing_share_null_rate,
    ROUND(AVG(CASE WHEN negative_lexicon_hits > 0 THEN 1 ELSE 0 END), 6) AS negative_signal_row_rate,
    ROUND(AVG(CAST(won_game AS DOUBLE)), 6) AS win_label_rate,
    ROUND(AVG(CAST(possible_tilt_label AS DOUBLE)), 6) AS tilt_label_rate
  FROM tf2.default.features_player_match
),
recent_form_alignment AS (
  SELECT
    ROUND(
      AVG(
        CASE
          WHEN frf.logid IS NOT NULL THEN 1
          ELSE 0
        END
      ),
      6
    ) AS join_coverage_rate
  FROM tf2.default.features_player_match fpm
  LEFT JOIN tf2.default.features_player_recent_form frf
    ON frf.logid = fpm.logid
   AND frf.steamid = fpm.steamid
),
players_with_history AS (
  SELECT
    ROUND(
      AVG(
        CASE
          WHEN games_played >= 20 THEN 1
          ELSE 0
        END
      ),
      6
    ) AS players_20_games_rate
  FROM (
    SELECT
      steamid,
      COUNT(*) AS games_played
    FROM tf2.default.features_player_match
    GROUP BY steamid
  ) player_games
),
snapshot_health AS (
  SELECT
    COUNT(*) AS snapshot_count,
    MAX(snapshot_cutoff_date) AS latest_snapshot_cutoff_date
  FROM tf2.default.ml_training_dataset_snapshots
),
checks AS (
  SELECT
    'feature_rows_minimum' AS check_name,
    CASE WHEN rows_total >= 10000 THEN 'PASS' ELSE 'FAIL' END AS status,
    CAST(rows_total AS DOUBLE) AS metric_value,
    '>= 10000 rows' AS threshold,
    'enough feature rows for baseline modelling' AS details
  FROM feature_coverage

  UNION ALL

  SELECT
    'distinct_players_minimum',
    CASE WHEN players_total >= 1000 THEN 'PASS' ELSE 'FAIL' END,
    CAST(players_total AS DOUBLE),
    '>= 1000 players',
    'enough unique players for generalisable training'
  FROM feature_coverage

  UNION ALL

  SELECT
    'distinct_days_minimum',
    CASE WHEN days_total >= 7 THEN 'PASS' ELSE 'FAIL' END,
    CAST(days_total AS DOUBLE),
    '>= 7 match days',
    'minimum temporal coverage for time-aware validation'
  FROM feature_coverage

  UNION ALL

  SELECT
    'map_diversity_minimum',
    CASE WHEN maps_total >= 50 THEN 'PASS' ELSE 'FAIL' END,
    CAST(maps_total AS DOUBLE),
    '>= 50 maps',
    'enough map diversity to reduce map-specific overfit'
  FROM feature_coverage

  UNION ALL

  SELECT
    'recent_form_join_coverage',
    CASE WHEN join_coverage_rate >= 0.995 THEN 'PASS' ELSE 'FAIL' END,
    join_coverage_rate,
    '>= 0.995',
    'recent-form features should exist for almost all match rows'
  FROM recent_form_alignment

  UNION ALL

  SELECT
    'impact_null_rate',
    CASE WHEN impact_null_rate <= 0.05 THEN 'PASS' ELSE 'FAIL' END,
    impact_null_rate,
    '<= 0.05',
    'impact feature missingness should stay low'
  FROM feature_coverage

  UNION ALL

  SELECT
    'kda_null_rate',
    CASE WHEN kda_null_rate <= 0.05 THEN 'PASS' ELSE 'FAIL' END,
    kda_null_rate,
    '<= 0.05',
    'KDA missingness should stay low'
  FROM feature_coverage

  UNION ALL

  SELECT
    'negative_chat_signal_rate',
    CASE WHEN negative_signal_row_rate >= 0.005 THEN 'PASS' ELSE 'FAIL' END,
    negative_signal_row_rate,
    '>= 0.005',
    'toxicity model needs non-zero negative-chat signal rows'
  FROM feature_coverage

  UNION ALL

  SELECT
    'tilt_label_rate_non_zero',
    CASE WHEN tilt_label_rate >= 0.005 THEN 'PASS' ELSE 'FAIL' END,
    tilt_label_rate,
    '>= 0.005',
    'tilt label must have enough positive examples'
  FROM feature_coverage

  UNION ALL

  SELECT
    'players_with_20_games_rate',
    CASE WHEN players_20_games_rate >= 0.20 THEN 'PASS' ELSE 'FAIL' END,
    players_20_games_rate,
    '>= 0.20',
    'history-heavy models need enough players with stable history'
  FROM players_with_history

  UNION ALL

  SELECT
    'snapshot_exists',
    CASE WHEN snapshot_count > 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(snapshot_count AS DOUBLE),
    '> 0 snapshots',
    'at least one immutable training snapshot should be materialised'
  FROM snapshot_health

  UNION ALL

  SELECT
    'snapshot_freshness_days',
    CASE
      WHEN latest_snapshot_cutoff_date IS NULL THEN 'FAIL'
      WHEN DATE_DIFF('day', latest_snapshot_cutoff_date, CURRENT_DATE) <= 2 THEN 'PASS'
      ELSE 'FAIL'
    END,
    CAST(DATE_DIFF('day', latest_snapshot_cutoff_date, CURRENT_DATE) AS DOUBLE),
    '<= 2 days',
    'latest training snapshot should stay reasonably fresh'
  FROM snapshot_health
)
SELECT
  check_name,
  status,
  metric_value,
  threshold,
  details
FROM checks
ORDER BY check_name;

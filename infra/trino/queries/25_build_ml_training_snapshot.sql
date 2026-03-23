-- Materialises a snapshot-scoped training dataset from features tables.
-- The script is idempotent for a given snapshot_id derived from max(match_time).

CREATE TABLE IF NOT EXISTS tf2.default.ml_training_dataset_snapshots (
  snapshot_id VARCHAR,
  snapshot_cutoff_time TIMESTAMP(6) WITH TIME ZONE,
  snapshot_cutoff_date DATE,
  source_match_rows BIGINT,
  source_recent_form_rows BIGINT,
  training_rows BIGINT,
  created_at TIMESTAMP(6) WITH TIME ZONE
)
WITH (
  format = 'PARQUET',
  partitioning = ARRAY['month(snapshot_cutoff_date)']
);

CREATE TABLE IF NOT EXISTS tf2.default.ml_training_player_match (
  snapshot_id VARCHAR,
  snapshot_cutoff_time TIMESTAMP(6) WITH TIME ZONE,
  snapshot_cutoff_date DATE,
  steamid VARCHAR,
  logid BIGINT,
  match_time TIMESTAMP(6) WITH TIME ZONE,
  match_date DATE,
  map VARCHAR,
  team VARCHAR,
  team_score BIGINT,
  opponent_score BIGINT,
  score_delta BIGINT,
  duration_seconds BIGINT,
  kills BIGINT,
  assists BIGINT,
  deaths BIGINT,
  damage_dealt BIGINT,
  healing_done BIGINT,
  ubers_used BIGINT,
  classes_played_count BIGINT,
  kill_share_of_team DOUBLE,
  damage_share_of_team DOUBLE,
  healing_share_of_team DOUBLE,
  impact_index DOUBLE,
  damage_per_minute DOUBLE,
  kda_ratio DOUBLE,
  chat_messages BIGINT,
  avg_message_length DOUBLE,
  all_caps_messages BIGINT,
  intense_punctuation_messages BIGINT,
  negative_lexicon_hits BIGINT,
  negative_chat_ratio DOUBLE,
  rolling_5_avg_kills DOUBLE,
  rolling_10_avg_damage DOUBLE,
  rolling_10_avg_impact DOUBLE,
  rolling_10_kda_ratio DOUBLE,
  rolling_10_win_rate DOUBLE,
  rolling_10_negative_chat_ratio DOUBLE,
  career_avg_kills DOUBLE,
  career_avg_damage DOUBLE,
  career_avg_impact DOUBLE,
  form_delta_kills DOUBLE,
  form_delta_damage DOUBLE,
  form_delta_impact DOUBLE,
  momentum_label VARCHAR,
  games_played_to_date BIGINT,
  label_win INTEGER,
  label_impact_percentile INTEGER,
  label_tilt INTEGER,
  created_at TIMESTAMP(6) WITH TIME ZONE
)
WITH (
  format = 'PARQUET',
  partitioning = ARRAY['snapshot_id']
);

INSERT INTO tf2.default.ml_training_dataset_snapshots (
  snapshot_id,
  snapshot_cutoff_time,
  snapshot_cutoff_date,
  source_match_rows,
  source_recent_form_rows,
  training_rows,
  created_at
)
WITH snapshot_candidate AS (
  SELECT
    CONCAT('train_', CAST(CAST(to_unixtime(MAX(match_time)) AS BIGINT) AS VARCHAR)) AS snapshot_id,
    CAST(MAX(match_time) AS TIMESTAMP(6) WITH TIME ZONE) AS snapshot_cutoff_time,
    CAST(MAX(match_time) AS DATE) AS snapshot_cutoff_date
  FROM tf2.default.features_player_match
),
counts AS (
  SELECT
    CAST((SELECT COUNT(*) FROM tf2.default.features_player_match) AS BIGINT) AS source_match_rows,
    CAST((SELECT COUNT(*) FROM tf2.default.features_player_recent_form) AS BIGINT) AS source_recent_form_rows
)
SELECT
  c.snapshot_id,
  c.snapshot_cutoff_time,
  c.snapshot_cutoff_date,
  ct.source_match_rows,
  ct.source_recent_form_rows,
  ct.source_match_rows AS training_rows,
  CURRENT_TIMESTAMP AS created_at
FROM snapshot_candidate c
CROSS JOIN counts ct
WHERE c.snapshot_cutoff_time IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM tf2.default.ml_training_dataset_snapshots s
    WHERE s.snapshot_id = c.snapshot_id
  );

INSERT INTO tf2.default.ml_training_player_match (
  snapshot_id,
  snapshot_cutoff_time,
  snapshot_cutoff_date,
  steamid,
  logid,
  match_time,
  match_date,
  map,
  team,
  team_score,
  opponent_score,
  score_delta,
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
  rolling_5_avg_kills,
  rolling_10_avg_damage,
  rolling_10_avg_impact,
  rolling_10_kda_ratio,
  rolling_10_win_rate,
  rolling_10_negative_chat_ratio,
  career_avg_kills,
  career_avg_damage,
  career_avg_impact,
  form_delta_kills,
  form_delta_damage,
  form_delta_impact,
  momentum_label,
  games_played_to_date,
  label_win,
  label_impact_percentile,
  label_tilt,
  created_at
)
WITH snapshot_candidate AS (
  SELECT
    CONCAT('train_', CAST(CAST(to_unixtime(MAX(match_time)) AS BIGINT) AS VARCHAR)) AS snapshot_id
  FROM tf2.default.features_player_match
),
logs_by_record AS (
  SELECT *
  FROM (
    SELECT
      l.*,
      ROW_NUMBER() OVER (PARTITION BY l.recordid ORDER BY l.__ingest_ts DESC) AS rn
    FROM tf2.default.logs l
  ) ranked
  WHERE rn = 1
),
logs_base AS (
  SELECT *
  FROM (
    SELECT
      l.*,
      ROW_NUMBER() OVER (
        PARTITION BY l.logid
        ORDER BY l.__ingest_ts DESC, l.sourcedateepochseconds DESC
      ) AS rn_log
    FROM logs_by_record l
  ) ranked
  WHERE rn_log = 1
),
selected_snapshot AS (
  SELECT
    s.snapshot_id,
    s.snapshot_cutoff_time,
    s.snapshot_cutoff_date
  FROM tf2.default.ml_training_dataset_snapshots s
  JOIN snapshot_candidate c
    ON c.snapshot_id = s.snapshot_id
  WHERE NOT EXISTS (
    SELECT 1
    FROM tf2.default.ml_training_player_match t
    WHERE t.snapshot_id = s.snapshot_id
  )
),
training_rows AS (
  SELECT
    ss.snapshot_id,
    ss.snapshot_cutoff_time,
    ss.snapshot_cutoff_date,
    fpm.steamid,
    CAST(fpm.logid AS BIGINT) AS logid,
    CAST(fpm.match_time AS TIMESTAMP(6) WITH TIME ZONE) AS match_time,
    fpm.match_date,
    fpm.map,
    fpm.team,
    CAST(
      CASE
        WHEN fpm.team = 'Red' THEN COALESCE(lb.redscore, 0)
        WHEN fpm.team = 'Blue' THEN COALESCE(lb.bluescore, 0)
        ELSE 0
      END AS BIGINT
    ) AS team_score,
    CAST(
      CASE
        WHEN fpm.team = 'Red' THEN COALESCE(lb.bluescore, 0)
        WHEN fpm.team = 'Blue' THEN COALESCE(lb.redscore, 0)
        ELSE 0
      END AS BIGINT
    ) AS opponent_score,
    CAST(
      CASE
        WHEN fpm.team = 'Red' THEN COALESCE(lb.redscore, 0) - COALESCE(lb.bluescore, 0)
        WHEN fpm.team = 'Blue' THEN COALESCE(lb.bluescore, 0) - COALESCE(lb.redscore, 0)
        ELSE 0
      END AS BIGINT
    ) AS score_delta,
    CAST(fpm.duration_seconds AS BIGINT) AS duration_seconds,
    CAST(fpm.kills AS BIGINT) AS kills,
    CAST(fpm.assists AS BIGINT) AS assists,
    CAST(fpm.deaths AS BIGINT) AS deaths,
    CAST(fpm.damage_dealt AS BIGINT) AS damage_dealt,
    CAST(fpm.healing_done AS BIGINT) AS healing_done,
    CAST(fpm.ubers_used AS BIGINT) AS ubers_used,
    CAST(fpm.classes_played_count AS BIGINT) AS classes_played_count,
    CAST(fpm.kill_share_of_team AS DOUBLE) AS kill_share_of_team,
    CAST(fpm.damage_share_of_team AS DOUBLE) AS damage_share_of_team,
    CAST(fpm.healing_share_of_team AS DOUBLE) AS healing_share_of_team,
    CAST(fpm.impact_index AS DOUBLE) AS impact_index,
    CAST(fpm.damage_per_minute AS DOUBLE) AS damage_per_minute,
    CAST(fpm.kda_ratio AS DOUBLE) AS kda_ratio,
    CAST(fpm.chat_messages AS BIGINT) AS chat_messages,
    CAST(fpm.avg_message_length AS DOUBLE) AS avg_message_length,
    CAST(fpm.all_caps_messages AS BIGINT) AS all_caps_messages,
    CAST(fpm.intense_punctuation_messages AS BIGINT) AS intense_punctuation_messages,
    CAST(fpm.negative_lexicon_hits AS BIGINT) AS negative_lexicon_hits,
    CAST(fpm.negative_chat_ratio AS DOUBLE) AS negative_chat_ratio,
    CAST(frf.rolling_5_avg_kills AS DOUBLE) AS rolling_5_avg_kills,
    CAST(frf.rolling_10_avg_damage AS DOUBLE) AS rolling_10_avg_damage,
    CAST(frf.rolling_10_avg_impact AS DOUBLE) AS rolling_10_avg_impact,
    CAST(frf.rolling_10_kda_ratio AS DOUBLE) AS rolling_10_kda_ratio,
    CAST(frf.rolling_10_win_rate AS DOUBLE) AS rolling_10_win_rate,
    CAST(frf.rolling_10_negative_chat_ratio AS DOUBLE) AS rolling_10_negative_chat_ratio,
    CAST(frf.career_avg_kills AS DOUBLE) AS career_avg_kills,
    CAST(frf.career_avg_damage AS DOUBLE) AS career_avg_damage,
    CAST(frf.career_avg_impact AS DOUBLE) AS career_avg_impact,
    CAST(frf.form_delta_kills AS DOUBLE) AS form_delta_kills,
    CAST(frf.form_delta_damage AS DOUBLE) AS form_delta_damage,
    CAST(frf.form_delta_impact AS DOUBLE) AS form_delta_impact,
    frf.momentum_label,
    CAST(frf.games_played_to_date AS BIGINT) AS games_played_to_date,
    CAST(fpm.won_game AS INTEGER) AS label_win,
    NTILE(100) OVER (
      PARTITION BY fpm.match_date
      ORDER BY fpm.impact_index, fpm.logid, fpm.steamid
    ) AS label_impact_percentile,
    CAST(fpm.possible_tilt_label AS INTEGER) AS label_tilt
  FROM tf2.default.features_player_match fpm
  JOIN selected_snapshot ss
    ON fpm.match_time <= ss.snapshot_cutoff_time
  LEFT JOIN logs_base lb
    ON lb.logid = fpm.logid
  LEFT JOIN tf2.default.features_player_recent_form frf
    ON frf.steamid = fpm.steamid
   AND frf.logid = fpm.logid
)
SELECT
  snapshot_id,
  snapshot_cutoff_time,
  snapshot_cutoff_date,
  steamid,
  logid,
  match_time,
  match_date,
  map,
  team,
  team_score,
  opponent_score,
  score_delta,
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
  rolling_5_avg_kills,
  rolling_10_avg_damage,
  rolling_10_avg_impact,
  rolling_10_kda_ratio,
  rolling_10_win_rate,
  rolling_10_negative_chat_ratio,
  career_avg_kills,
  career_avg_damage,
  career_avg_impact,
  form_delta_kills,
  form_delta_damage,
  form_delta_impact,
  momentum_label,
  games_played_to_date,
  label_win,
  label_impact_percentile,
  label_tilt,
  CURRENT_TIMESTAMP AS created_at
FROM training_rows;

-- Incrementally refreshes per-player, per-match features from core tables.
-- Strategy: recalculate a rolling date window and upsert by logid.

CREATE TABLE IF NOT EXISTS tf2.default.features_player_match
WITH (
  format = 'PARQUET',
  partitioning = ARRAY['month(match_date)']
) AS
WITH logs_by_record AS (
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
summaries_by_record AS (
  SELECT *
  FROM (
    SELECT
      s.*,
      ROW_NUMBER() OVER (PARTITION BY s.recordid ORDER BY s.__ingest_ts DESC) AS rn
    FROM tf2.default.summaries s
  ) ranked
  WHERE rn = 1
),
summaries_base AS (
  SELECT *
  FROM (
    SELECT
      s.*,
      ROW_NUMBER() OVER (
        PARTITION BY s.logid, s.steamid
        ORDER BY s.__ingest_ts DESC, s.sourcedateepochseconds DESC
      ) AS rn_player
    FROM summaries_by_record s
    WHERE s.team IN ('Red', 'Blue')
  ) ranked
  WHERE rn_player = 1
),
messages_base AS (
  SELECT *
  FROM (
    SELECT
      m.*,
      ROW_NUMBER() OVER (PARTITION BY m.recordid ORDER BY m.__ingest_ts DESC) AS rn
    FROM tf2.default.messages m
  ) ranked
  WHERE rn = 1
),
team_totals AS (
  SELECT
    logid,
    team,
    SUM(COALESCE(kills, 0)) AS team_kills,
    SUM(COALESCE(damagedealt, 0)) AS team_damage,
    SUM(COALESCE(healingdone, 0)) AS team_healing
  FROM summaries_base
  GROUP BY logid, team
),
chat_by_player_game AS (
  SELECT
    steamid,
    logid,
    COUNT(*) AS chat_messages,
    AVG(LENGTH(message)) AS avg_message_length,
    SUM(CASE WHEN REGEXP_LIKE(message, '^[A-Z0-9 !?.''-]+$') THEN 1 ELSE 0 END) AS all_caps_messages,
    SUM(CASE WHEN REGEXP_LIKE(messagelower, '!{2,}') THEN 1 ELSE 0 END) AS intense_punctuation_messages,
    SUM(
      CASE
        WHEN REGEXP_LIKE(
          messagelower,
          '\\b(noob|trash|idiot|stupid|cheat|cheater|ez|wtf|losing|throw|threw|report)\\b'
        ) THEN 1
        ELSE 0
      END
    ) AS negative_lexicon_hits
  FROM messages_base
  WHERE steamid IS NOT NULL
  GROUP BY steamid, logid
)
SELECT
  s.logid,
  s.steamid,
  s.map,
  s.team,
  from_iso8601_timestamp(s.sourcedateiso) AS match_time,
  CAST(from_iso8601_timestamp(s.sourcedateiso) AS DATE) AS match_date,
  COALESCE(l.durationseconds, 0) AS duration_seconds,
  COALESCE(s.kills, 0) AS kills,
  COALESCE(s.assists, 0) AS assists,
  COALESCE(s.deaths, 0) AS deaths,
  COALESCE(s.damagedealt, 0) AS damage_dealt,
  COALESCE(s.healingdone, 0) AS healing_done,
  COALESCE(s.ubersused, 0) AS ubers_used,
  COALESCE(s.classesplayedcsv, '') AS classes_played_csv,
  CARDINALITY(
    FILTER(
      TRANSFORM(SPLIT(COALESCE(s.classesplayedcsv, ''), ','), c -> TRIM(c)),
      c -> c <> ''
    )
  ) AS classes_played_count,
  CASE
    WHEN s.team = 'Red' AND l.redscore > l.bluescore THEN 1
    WHEN s.team = 'Blue' AND l.bluescore > l.redscore THEN 1
    ELSE 0
  END AS won_game,
  ROUND(COALESCE(s.kills, 0) / NULLIF(CAST(tt.team_kills AS DOUBLE), 0), 4) AS kill_share_of_team,
  ROUND(COALESCE(s.damagedealt, 0) / NULLIF(CAST(tt.team_damage AS DOUBLE), 0), 4) AS damage_share_of_team,
  ROUND(COALESCE(s.healingdone, 0) / NULLIF(CAST(tt.team_healing AS DOUBLE), 0), 4) AS healing_share_of_team,
  ROUND(
    0.45 * (COALESCE(s.damagedealt, 0) / NULLIF(CAST(tt.team_damage AS DOUBLE), 0))
      + 0.35 * (COALESCE(s.kills, 0) / NULLIF(CAST(tt.team_kills AS DOUBLE), 0))
      + 0.20 * (COALESCE(s.healingdone, 0) / NULLIF(CAST(tt.team_healing AS DOUBLE), 0)),
    4
  ) AS impact_index,
  ROUND(
    COALESCE(s.damagedealt, 0) / NULLIF(COALESCE(l.durationseconds, 0) / 60.0, 0),
    3
  ) AS damage_per_minute,
  ROUND(
    (COALESCE(s.kills, 0) + COALESCE(s.assists, 0))
    / NULLIF(CAST(COALESCE(s.deaths, 0) AS DOUBLE), 0),
    3
  ) AS kda_ratio,
  COALESCE(cbpg.chat_messages, 0) AS chat_messages,
  ROUND(COALESCE(cbpg.avg_message_length, 0.0), 3) AS avg_message_length,
  COALESCE(cbpg.all_caps_messages, 0) AS all_caps_messages,
  COALESCE(cbpg.intense_punctuation_messages, 0) AS intense_punctuation_messages,
  COALESCE(cbpg.negative_lexicon_hits, 0) AS negative_lexicon_hits,
  CASE
    WHEN COALESCE(cbpg.chat_messages, 0) = 0 THEN 0.0
    ELSE CAST(COALESCE(cbpg.negative_lexicon_hits, 0) AS DOUBLE) / cbpg.chat_messages
  END AS negative_chat_ratio,
  CASE
    WHEN COALESCE(s.deaths, 0) >= 20 AND COALESCE(cbpg.negative_lexicon_hits, 0) >= 2 THEN 1
    ELSE 0
  END AS possible_tilt_label
FROM summaries_base s
LEFT JOIN logs_base l ON l.logid = s.logid
LEFT JOIN team_totals tt
  ON tt.logid = s.logid
 AND tt.team = s.team
LEFT JOIN chat_by_player_game cbpg
  ON cbpg.logid = s.logid
 AND cbpg.steamid = s.steamid;

DELETE FROM tf2.default.features_player_match
WHERE logid IN (
  WITH summaries_by_record AS (
    SELECT *
    FROM (
      SELECT
        s.*,
        ROW_NUMBER() OVER (PARTITION BY s.recordid ORDER BY s.__ingest_ts DESC) AS rn
      FROM tf2.default.summaries s
    ) ranked
    WHERE rn = 1
  ),
  summaries_base AS (
    SELECT *
    FROM (
      SELECT
        s.*,
        ROW_NUMBER() OVER (
          PARTITION BY s.logid, s.steamid
          ORDER BY s.__ingest_ts DESC, s.sourcedateepochseconds DESC
        ) AS rn_player
      FROM summaries_by_record s
      WHERE s.team IN ('Red', 'Blue')
    ) ranked
    WHERE rn_player = 1
  )
  SELECT DISTINCT s.logid
  FROM summaries_base s
  CROSS JOIN (
    SELECT COALESCE(
      DATE_ADD('day', -7, MAX(CAST(from_iso8601_timestamp(sourcedateiso) AS DATE))),
      DATE '1970-01-01'
    ) AS refresh_start_date
    FROM summaries_base
  ) b
  WHERE CAST(from_iso8601_timestamp(s.sourcedateiso) AS DATE) >= b.refresh_start_date
);

INSERT INTO tf2.default.features_player_match
WITH logs_by_record AS (
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
summaries_by_record AS (
  SELECT *
  FROM (
    SELECT
      s.*,
      ROW_NUMBER() OVER (PARTITION BY s.recordid ORDER BY s.__ingest_ts DESC) AS rn
    FROM tf2.default.summaries s
  ) ranked
  WHERE rn = 1
),
summaries_base AS (
  SELECT *
  FROM (
    SELECT
      s.*,
      ROW_NUMBER() OVER (
        PARTITION BY s.logid, s.steamid
        ORDER BY s.__ingest_ts DESC, s.sourcedateepochseconds DESC
      ) AS rn_player
    FROM summaries_by_record s
    WHERE s.team IN ('Red', 'Blue')
  ) ranked
  WHERE rn_player = 1
),
messages_base AS (
  SELECT *
  FROM (
    SELECT
      m.*,
      ROW_NUMBER() OVER (PARTITION BY m.recordid ORDER BY m.__ingest_ts DESC) AS rn
    FROM tf2.default.messages m
  ) ranked
  WHERE rn = 1
),
bounds AS (
  SELECT COALESCE(
    DATE_ADD('day', -7, MAX(CAST(from_iso8601_timestamp(sourcedateiso) AS DATE))),
    DATE '1970-01-01'
  ) AS refresh_start_date
  FROM summaries_base
),
changed_logs AS (
  SELECT DISTINCT s.logid
  FROM summaries_base s
  CROSS JOIN bounds b
  WHERE CAST(from_iso8601_timestamp(s.sourcedateiso) AS DATE) >= b.refresh_start_date
),
team_totals AS (
  SELECT
    s.logid,
    s.team,
    SUM(COALESCE(s.kills, 0)) AS team_kills,
    SUM(COALESCE(s.damagedealt, 0)) AS team_damage,
    SUM(COALESCE(s.healingdone, 0)) AS team_healing
  FROM summaries_base s
  JOIN changed_logs cl ON cl.logid = s.logid
  GROUP BY s.logid, s.team
),
chat_by_player_game AS (
  SELECT
    m.steamid,
    m.logid,
    COUNT(*) AS chat_messages,
    AVG(LENGTH(m.message)) AS avg_message_length,
    SUM(CASE WHEN REGEXP_LIKE(m.message, '^[A-Z0-9 !?.''-]+$') THEN 1 ELSE 0 END) AS all_caps_messages,
    SUM(CASE WHEN REGEXP_LIKE(m.messagelower, '!{2,}') THEN 1 ELSE 0 END) AS intense_punctuation_messages,
    SUM(
      CASE
        WHEN REGEXP_LIKE(
          m.messagelower,
          '\\b(noob|trash|idiot|stupid|cheat|cheater|ez|wtf|losing|throw|threw|report)\\b'
        ) THEN 1
        ELSE 0
      END
    ) AS negative_lexicon_hits
  FROM messages_base m
  JOIN changed_logs cl ON cl.logid = m.logid
  WHERE m.steamid IS NOT NULL
  GROUP BY m.steamid, m.logid
)
SELECT
  s.logid,
  s.steamid,
  s.map,
  s.team,
  from_iso8601_timestamp(s.sourcedateiso) AS match_time,
  CAST(from_iso8601_timestamp(s.sourcedateiso) AS DATE) AS match_date,
  COALESCE(l.durationseconds, 0) AS duration_seconds,
  COALESCE(s.kills, 0) AS kills,
  COALESCE(s.assists, 0) AS assists,
  COALESCE(s.deaths, 0) AS deaths,
  COALESCE(s.damagedealt, 0) AS damage_dealt,
  COALESCE(s.healingdone, 0) AS healing_done,
  COALESCE(s.ubersused, 0) AS ubers_used,
  COALESCE(s.classesplayedcsv, '') AS classes_played_csv,
  CARDINALITY(
    FILTER(
      TRANSFORM(SPLIT(COALESCE(s.classesplayedcsv, ''), ','), c -> TRIM(c)),
      c -> c <> ''
    )
  ) AS classes_played_count,
  CASE
    WHEN s.team = 'Red' AND l.redscore > l.bluescore THEN 1
    WHEN s.team = 'Blue' AND l.bluescore > l.redscore THEN 1
    ELSE 0
  END AS won_game,
  ROUND(COALESCE(s.kills, 0) / NULLIF(CAST(tt.team_kills AS DOUBLE), 0), 4) AS kill_share_of_team,
  ROUND(COALESCE(s.damagedealt, 0) / NULLIF(CAST(tt.team_damage AS DOUBLE), 0), 4) AS damage_share_of_team,
  ROUND(COALESCE(s.healingdone, 0) / NULLIF(CAST(tt.team_healing AS DOUBLE), 0), 4) AS healing_share_of_team,
  ROUND(
    0.45 * (COALESCE(s.damagedealt, 0) / NULLIF(CAST(tt.team_damage AS DOUBLE), 0))
      + 0.35 * (COALESCE(s.kills, 0) / NULLIF(CAST(tt.team_kills AS DOUBLE), 0))
      + 0.20 * (COALESCE(s.healingdone, 0) / NULLIF(CAST(tt.team_healing AS DOUBLE), 0)),
    4
  ) AS impact_index,
  ROUND(
    COALESCE(s.damagedealt, 0) / NULLIF(COALESCE(l.durationseconds, 0) / 60.0, 0),
    3
  ) AS damage_per_minute,
  ROUND(
    (COALESCE(s.kills, 0) + COALESCE(s.assists, 0))
    / NULLIF(CAST(COALESCE(s.deaths, 0) AS DOUBLE), 0),
    3
  ) AS kda_ratio,
  COALESCE(cbpg.chat_messages, 0) AS chat_messages,
  ROUND(COALESCE(cbpg.avg_message_length, 0.0), 3) AS avg_message_length,
  COALESCE(cbpg.all_caps_messages, 0) AS all_caps_messages,
  COALESCE(cbpg.intense_punctuation_messages, 0) AS intense_punctuation_messages,
  COALESCE(cbpg.negative_lexicon_hits, 0) AS negative_lexicon_hits,
  CASE
    WHEN COALESCE(cbpg.chat_messages, 0) = 0 THEN 0.0
    ELSE CAST(COALESCE(cbpg.negative_lexicon_hits, 0) AS DOUBLE) / cbpg.chat_messages
  END AS negative_chat_ratio,
  CASE
    WHEN COALESCE(s.deaths, 0) >= 20 AND COALESCE(cbpg.negative_lexicon_hits, 0) >= 2 THEN 1
    ELSE 0
  END AS possible_tilt_label
FROM summaries_base s
JOIN changed_logs cl ON cl.logid = s.logid
LEFT JOIN logs_base l ON l.logid = s.logid
LEFT JOIN team_totals tt
  ON tt.logid = s.logid
 AND tt.team = s.team
LEFT JOIN chat_by_player_game cbpg
  ON cbpg.logid = s.logid
 AND cbpg.steamid = s.steamid;

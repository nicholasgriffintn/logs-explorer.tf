-- Rebuilds per-player, per-match features directly from core tables.
DROP TABLE IF EXISTS tf2.default.features_player_match;

CREATE TABLE tf2.default.features_player_match
WITH (
  format = 'PARQUET',
  partitioning = ARRAY['month(match_date)']
) AS
WITH team_totals AS (
  SELECT
    logid,
    team,
    SUM(COALESCE(kills, 0)) AS team_kills,
    SUM(COALESCE(damagedealt, 0)) AS team_damage,
    SUM(COALESCE(healingdone, 0)) AS team_healing
  FROM tf2.default.summaries
  WHERE team IN ('Red', 'Blue')
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
          '\b(noob|trash|idiot|stupid|cheat|cheater|ez|wtf|losing|throw|threw|report)\b'
        ) THEN 1
        ELSE 0
      END
    ) AS negative_lexicon_hits
  FROM tf2.default.messages
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
FROM tf2.default.summaries s
LEFT JOIN tf2.default.logs l ON l.logid = s.logid
LEFT JOIN team_totals tt
  ON tt.logid = s.logid
 AND tt.team = s.team
LEFT JOIN chat_by_player_game cbpg
  ON cbpg.logid = s.logid
 AND cbpg.steamid = s.steamid
WHERE s.team IN ('Red', 'Blue');

WITH player_game AS (
  SELECT
    s.steamid,
    s.logid,
    s.map,
    s.team,
    from_iso8601_timestamp(s.sourcedateiso) AS match_time,
    COALESCE(s.kills, 0) AS kills,
    COALESCE(s.assists, 0) AS assists,
    COALESCE(s.deaths, 0) AS deaths,
    COALESCE(s.damagedealt, 0) AS damage_dealt,
    COALESCE(s.healingdone, 0) AS healing_done,
    COALESCE(s.ubersused, 0) AS ubers_used,
    COALESCE(s.classesplayedcsv, '') AS classes_played_csv
  FROM tf2.default.summaries s
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
          '\b(noob|trash|idiot|stupid|cheat|cheater|ez|wtf|losing|throw|threw|report)\b'
        ) THEN 1
        ELSE 0
      END
    ) AS negative_lexicon_hits
  FROM tf2.default.messages m
  WHERE m.steamid IS NOT NULL
  GROUP BY m.steamid, m.logid
),
joined AS (
  SELECT
    pg.steamid,
    pg.logid,
    pg.map,
    pg.team,
    pg.match_time,
    pg.kills,
    pg.assists,
    pg.deaths,
    pg.damage_dealt,
    pg.healing_done,
    pg.ubers_used,
    pg.classes_played_csv,
    COALESCE(cbpg.chat_messages, 0) AS chat_messages,
    COALESCE(cbpg.avg_message_length, 0.0) AS avg_message_length,
    COALESCE(cbpg.all_caps_messages, 0) AS all_caps_messages,
    COALESCE(cbpg.intense_punctuation_messages, 0) AS intense_punctuation_messages,
    COALESCE(cbpg.negative_lexicon_hits, 0) AS negative_lexicon_hits,
    l.redscore,
    l.bluescore
  FROM player_game pg
  LEFT JOIN chat_by_player_game cbpg
    ON cbpg.steamid = pg.steamid
   AND cbpg.logid = pg.logid
  LEFT JOIN tf2.default.logs l ON l.logid = pg.logid
)
SELECT
  steamid,
  logid,
  match_time,
  map,
  team,
  kills,
  assists,
  deaths,
  damage_dealt,
  healing_done,
  ubers_used,
  classes_played_csv,
  chat_messages,
  ROUND(avg_message_length, 2) AS avg_message_length,
  all_caps_messages,
  intense_punctuation_messages,
  negative_lexicon_hits,
  CASE
    WHEN team = 'Red' AND redscore > bluescore THEN 1
    WHEN team = 'Blue' AND bluescore > redscore THEN 1
    ELSE 0
  END AS won_game,
  CASE
    WHEN chat_messages = 0 THEN 0.0
    ELSE CAST(negative_lexicon_hits AS DOUBLE) / chat_messages
  END AS negative_chat_ratio,
  CASE
    WHEN deaths >= 20 AND negative_lexicon_hits >= 2 THEN 1
    ELSE 0
  END AS possible_tilt_label
FROM joined
ORDER BY match_time DESC;

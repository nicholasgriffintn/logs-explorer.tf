WITH params AS (
  -- Lower this threshold if your dataset is still small.
  SELECT 25 AS min_games
),
player_games AS (
  SELECT
    steamid,
    logid,
    classesplayedcsv
  FROM tf2.default.summaries
),
class_rows AS (
  SELECT
    pg.steamid,
    pg.logid,
    LOWER(TRIM(class_name)) AS class_name
  FROM player_games pg
  CROSS JOIN UNNEST(SPLIT(COALESCE(pg.classesplayedcsv, ''), ',')) AS t(class_name)
  WHERE TRIM(class_name) <> ''
),
class_counts AS (
  SELECT
    steamid,
    class_name,
    COUNT(DISTINCT logid) AS class_games
  FROM class_rows
  GROUP BY steamid, class_name
),
player_totals AS (
  SELECT
    steamid,
    COUNT(*) AS total_games
  FROM player_games
  GROUP BY steamid
  HAVING COUNT(*) >= (SELECT min_games FROM params)
),
player_class_profile AS (
  SELECT
    cc.steamid,
    COUNT(*) AS unique_classes,
    MAX_BY(cc.class_name, cc.class_games) AS primary_class,
    MAX(cc.class_games) AS primary_class_games,
    SUM(cc.class_games) AS total_class_game_mentions
  FROM class_counts cc
  GROUP BY cc.steamid
)
SELECT
  pt.steamid,
  pt.total_games,
  COALESCE(pcp.unique_classes, 0) AS unique_classes,
  pcp.primary_class,
  pcp.primary_class_games,
  ROUND(
    COALESCE(pcp.unique_classes, 0) / NULLIF(CAST(pt.total_games AS DOUBLE), 0),
    3
  ) AS class_diversity_index,
  ROUND(
    COALESCE(pcp.primary_class_games, 0) / NULLIF(CAST(pt.total_games AS DOUBLE), 0),
    3
  ) AS primary_class_share
FROM player_totals pt
LEFT JOIN player_class_profile pcp ON pcp.steamid = pt.steamid
ORDER BY class_diversity_index DESC, total_games DESC
LIMIT 300;

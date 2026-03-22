# Trino analytics query pack

These queries are designed for advanced TF2 analysis across players, games, and chat.
They assume your Trino catalog is `tf2` and the tables are:

- `tf2.default.logs`
- `tf2.default.summaries`
- `tf2.default.messages`

If your catalog namespace differs, replace the table references after running:

```sql
SHOW SCHEMAS FROM tf2;
SHOW TABLES FROM tf2.default;
```

## Query index

- `00_log_player_breakdown.sql`: one-log deep dive across all player IDs (default `4031015`)
- `01_player_activity_baseline.sql`: per-player career baselines and win/KD/impact metrics
- `02_player_form_and_momentum.sql`: per-game trendline for one player with rolling form stats
- `03_player_relative_impact.sql`: one player's contribution relative to team totals by game
- `04_player_pair_synergy.sql`: two-player synergy scores for duos (optionally anchor to one player)
- `05_head_to_head_two_players.sql`: direct comparison for two specific player IDs
- `06_map_specialists.sql`: map-specific uplift versus each player's own baseline
- `07_game_competitiveness_and_pace.sql`: map-level game quality and pace metrics
- `08_class_usage_and_flexibility.sql`: class breadth and playstyle flexibility per player
- `09_player_coplay_network.sql`: who a target player appears with and against most often
- `10_sentiment_feature_export.sql`: feature export for a downstream sentiment/behaviour model

## Usage notes

- Queries include a `params` CTE when input is needed; replace sample Steam IDs first.
- Most queries include minimum-game thresholds; tune these before drawing conclusions.
- These are written to run on full history; add date filters for faster iteration.

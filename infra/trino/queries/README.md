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
- `11_build_features_player_match.sql`: build match-level feature table from `logs/messages/summaries`
- `12_build_features_player_recent_form.sql`: build rolling form and momentum features per player
- `13_build_serving_player_profiles.sql`: build serving table for per-player dashboard/profile reads
- `14_build_serving_map_overview_daily.sql`: build serving table for map/day dashboard reads

## Usage notes

- Queries include a `params` CTE when input is needed; replace sample Steam IDs and log IDs first.
- Most queries include minimum-game thresholds; lower these if your dataset is still small.
- These are written to run on full history; add date filters for faster iteration.

## Full processing flow

If you want to build `features` and `serving` layers on top of your existing core tables, run:

```bash
docker exec -i tf2-trino trino < infra/trino/queries/11_build_features_player_match.sql
docker exec -i tf2-trino trino < infra/trino/queries/12_build_features_player_recent_form.sql
docker exec -i tf2-trino trino < infra/trino/queries/13_build_serving_player_profiles.sql
docker exec -i tf2-trino trino < infra/trino/queries/14_build_serving_map_overview_daily.sql
```

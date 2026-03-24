# Trino analytics query pack

These queries are designed for TF2 analysis and dashboard serving reads.
Trino is query/serving only. Spark owns processing and refresh.

## Source tables assumed

- `tf2.default.logs`
- `tf2.default.summaries`
- `tf2.default.messages`
- Spark-produced `features_*`, `serving_*`, and ML serving tables

If your catalog namespace differs, replace table references after running:

```sql
SHOW SCHEMAS FROM tf2;
SHOW TABLES FROM tf2.default;
```

## Query index

Exploration (`infra/trino/queries/exploration`):

- `log_player_breakdown.sql`: one-log deep dive across all player IDs
- `player_activity_baseline.sql`: per-player career baselines and win/KD/impact metrics
- `player_form_and_momentum.sql`: per-game trendline for one player with rolling form stats
- `player_relative_impact.sql`: one player contribution relative to team totals by game
- `player_pair_synergy.sql`: two-player synergy scores for duos
- `head_to_head_two_players.sql`: direct comparison for two specific player IDs
- `map_specialists.sql`: map-specific uplift versus each player baseline
- `game_competitiveness_and_pace.sql`: map-level game quality and pace metrics
- `class_usage_and_flexibility.sql`: class breadth and playstyle flexibility per player
- `player_coplay_network.sql`: who a target player appears with and against most often
- `sentiment_feature_export.sql`: feature export for downstream sentiment/behaviour models

Dashboard (`infra/trino/queries/dashboard`):

- `dashboard_player_profile_and_momentum.sql`
- `dashboard_map_competitiveness_and_pace.sql`
- `dashboard_chat_behaviour_and_tilt_risk.sql`
- `dashboard_player_match_deep_dive.sql`
- `dashboard_ml_progress_and_registry.sql`
- `dashboard_ml_prediction_quality.sql`
- `dashboard_player_synergy_network.sql`
- `dashboard_map_tilt_anomalies.sql`

Quality (`infra/trino/queries/quality`):

- `data_quality_checks.sql`: quality gate checks with PASS/FAIL thresholds
- `serving_query_performance_benchmark.sql`: benchmark pack (`EXPLAIN ANALYZE`) for serving query latency checks

Ops (`infra/trino/queries/ops`):

- `run_iceberg_maintenance.sh`: compaction + snapshot expiry for existing Iceberg tables

ML (`infra/trino/queries/ml`):

- `ml_data_readiness_check.sql`: ML data quality/readiness checks
- `run_ml_baseline_training.sh`: baseline model training + candidate registry upsert
- `run_ml_readiness_check.sh`: one-command ML readiness checks
- `run_ml_model_stage_transition.sh`: stage transition helper + history write
- `run_ml_model_rollback.sh`: rollback helper to repromote a model version

## Processing and refresh

Run Spark feature-serving processing before executing serving/dashboard queries:

```bash
infra/spark/run_feature_pipeline.sh incremental
```

If you need ML snapshot/registry progress tables refreshed, run the ML pipeline separately:

```bash
infra/spark/run_ml_pipeline.sh incremental
```

Run serving quality checks after refresh:

```bash
docker exec -i tf2-trino trino < infra/trino/queries/quality/data_quality_checks.sql
```

Run Iceberg table maintenance on a regular cadence (for example weekly):

```bash
infra/trino/queries/ops/run_iceberg_maintenance.sh
```

Operational details and failure recovery are documented in:

- `/docs/refresh-operations-runbook.md`
- `/docs/data-platform-e2e-workflow.md`

## Dashboard starter pack

Use these `serving_*`-only queries for initial dashboard tiles:

```bash
docker exec -i tf2-trino trino < infra/trino/queries/dashboard/dashboard_player_profile_and_momentum.sql
docker exec -i tf2-trino trino < infra/trino/queries/dashboard/dashboard_map_competitiveness_and_pace.sql
docker exec -i tf2-trino trino < infra/trino/queries/dashboard/dashboard_chat_behaviour_and_tilt_risk.sql
docker exec -i tf2-trino trino < infra/trino/queries/dashboard/dashboard_player_match_deep_dive.sql
docker exec -i tf2-trino trino < infra/trino/queries/dashboard/dashboard_ml_progress_and_registry.sql
docker exec -i tf2-trino trino < infra/trino/queries/dashboard/dashboard_ml_prediction_quality.sql
docker exec -i tf2-trino trino < infra/trino/queries/dashboard/dashboard_player_synergy_network.sql
docker exec -i tf2-trino trino < infra/trino/queries/dashboard/dashboard_map_tilt_anomalies.sql
```

Set query parameters in each file `params` CTE before running.

For Superset setup and Trino datasource wiring, use:

- `/infra/superset/README.md`

# Trino analytics query pack

These queries support analytics and dashboard serving.
Trino is the query-serving and validation plane; Airflow orchestrates quality/readiness/maintenance execution via SQL operators.

## Source tables assumed

- `tf2.default.logs`
- `tf2.default.summaries`
- `tf2.default.messages`
- Spark-produced `features_*`, `serving_*`, and ML serving tables

If your catalog namespace differs, update table references after:

```sql
SHOW SCHEMAS FROM tf2;
SHOW TABLES FROM tf2.default;
```

## Query index

Exploration (`infra/trino/queries/exploration`):

- `log_player_breakdown.sql`
- `player_activity_baseline.sql`
- `player_form_and_momentum.sql`
- `player_relative_impact.sql`
- `player_pair_synergy.sql`
- `head_to_head_two_players.sql`
- `map_specialists.sql`
- `game_competitiveness_and_pace.sql`
- `class_usage_and_flexibility.sql`
- `player_coplay_network.sql`
- `sentiment_feature_export.sql`

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

- `data_quality_checks.sql`
- `serving_query_performance_benchmark.sql`

Ops (`infra/trino/queries/ops`):

- Iceberg maintenance SQL run via Airflow maintenance DAG

ML (`infra/trino/queries/ml`):

- `ml_data_readiness_check.sql`
- `run_ml_promotion_gate_check.sh`
- `run_ml_model_stage_transition.sh`
- `run_ml_model_rollback.sh`

## Operational model

- Refresh and validation execution runs through Airflow DAGs.
- Dashboards and ad-hoc analytics use Trino directly.
- For lifecycle triggers, use `infra/airflow/README.md` and `docs/data-platform-e2e-workflow.md`.

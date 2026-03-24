# Dashboard dataset contract for serving tables

This document defines stable contracts for dashboard queries. Dashboards should read only from `serving_*` tables.

## Scope

Contract tables:

- `tf2.default.serving_player_profiles`
- `tf2.default.serving_map_overview_daily`
- `tf2.default.serving_player_match_deep_dive`
- `tf2.default.serving_ml_model_registry`
- `tf2.default.serving_ml_pipeline_progress_daily`

Non-contract tables (`core`, `features`) may evolve more quickly and should not be direct dashboard dependencies.

## Freshness and publish policy

- Refresh cadence: feature-serving Spark pipeline (`infra/spark/run_feature_pipeline.sh incremental`) daily or more often.
- ML progress cadence: separate ML Spark pipeline (`infra/spark/run_ml_pipeline.sh incremental`) on its own schedule.
- Publish gate: `data_quality_checks.sql` must be all `PASS`.
- SLA target: serving query P95 under 3 seconds for dashboard slices.

## `serving_player_profiles`

Grain: one row per `steamid`.

Key columns:

- **Identity and activity window**: `steamid`, `games_played`, `maps_played`, `first_seen_at`, `last_seen_at`
- **Long-run baseline**: `career_win_rate`, `career_kd_ratio`, `career_avg_kda_ratio`, `career_avg_kills`, `career_avg_damage`, `career_avg_damage_per_minute`, `career_avg_impact`
- **Behaviour and risk**: `tilt_risk_rate`, `rolling_10_negative_chat_ratio`
- **Short-run form**: `rolling_5_avg_kills`, `rolling_10_avg_damage`, `rolling_10_avg_impact`, `form_delta_kills`, `form_delta_damage`, `form_delta_impact`, `momentum_label`
- **Recent window summary**: `recent_30_avg_kills`, `recent_30_avg_damage`, `recent_30_avg_impact`, `recent_30_win_rate`
- **Metadata**: `updated_at`

Contract rules:

- Primary key is `steamid`.
- One row per player with at least one observed match in `features_player_match`.
- `momentum_label` uses calibrated cutoffs on `form_delta_impact`: `hot >= 0.016`, `cold <= -0.020`, otherwise `stable`.
- Breaking changes require versioned table rollout (for example `serving_player_profiles_v2`).

## `serving_map_overview_daily`

Grain: one row per `map` and `match_date`.

Key columns:

- **Volume and pace**: `games`, `avg_duration_minutes`, `avg_total_kills`, `avg_kills_per_minute`
- **Match quality**: `close_game_rate`, `blowout_rate`
- **Behavioural overlays**: `active_players`, `avg_player_impact_index`, `avg_negative_chat_ratio`, `tilt_signal_rate`
- **Metadata**: `updated_at`

Contract rules:

- Composite key is `(map, match_date)`.
- `match_date` is UTC date derived from source timestamp.
- When rolling up `close_game_rate` and `blowout_rate` across rows, use game-weighted aggregation: `SUM(rate * games) / SUM(games)`.
- Breaking schema updates require versioned table rollout.

## `serving_player_match_deep_dive`

Grain: one row per `(logid, steamid)`.

Key columns:

- **Match identity**: `logid`, `steamid`, `match_time`, `match_date`, `map`, `team`, `won_game`
- **Per-match contribution**: `kills`, `assists`, `deaths`, `damage_dealt`, `healing_done`, `ubers_used`, `impact_index`, `damage_per_minute`, `kda_ratio`
- **Team-share and behaviour detail**: `kill_share_of_team`, `damage_share_of_team`, `healing_share_of_team`, `chat_messages`, `negative_chat_ratio`, `possible_tilt_label`
- **Rolling context**: `rolling_5_avg_kills`, `rolling_10_avg_damage`, `rolling_10_avg_impact`, `rolling_10_win_rate`, `form_delta_impact`, `momentum_label`
- **Dashboard-friendly buckets**: `behaviour_risk_tier`, `impact_tier`
- **Metadata**: `updated_at`

Contract rules:

- Composite key is `(logid, steamid)`.
- Table is a serving projection of `features_player_match` and `features_player_recent_form`; it must not depend on raw `core` tables in dashboard paths.
- `impact_tier` cutoffs are `elite >= 0.2400`, `strong >= 0.1800`, `average >= 0.1300`, else `struggling`.
- Breaking schema updates require versioned table rollout.

## `serving_ml_model_registry`

Grain: one row per model version (`model_name`, `model_version`).

Key columns:

- **Version identity**: `model_name`, `model_version`, `task_type`, `stage`, `is_active`
- **Lineage**: `snapshot_id`, `snapshot_cutoff_time`, `snapshot_cutoff_date`, `snapshot_training_rows`, `training_code_version`, `feature_sql_version`
- **Performance fields**: `primary_metric_name`, `primary_metric_value`, `metric_auc`, `metric_precision`, `metric_recall`, `metric_f1`, `metric_rmse`, `metric_mae`
- **Lifecycle metadata**: `created_at`, `promoted_at`, `model_age_days`, `data_age_days`, `updated_at`

Contract rules:

- Composite key is `(model_name, model_version)`.
- Metrics are parsed from `metrics_json` when present and left nullable when unavailable.

## `serving_ml_pipeline_progress_daily`

Grain: one row per `progress_date`.

Key columns:

- **Refresh execution**: `pipeline_success_runs`, `pipeline_failed_runs`, `avg_pipeline_duration_seconds`, `latest_pipeline_status`
- **Training progress**: `snapshots_created`, `training_rows_materialised`, `latest_snapshot_id`, `latest_snapshot_created_at`
- **Registry activity**: `models_registered`, `candidate_models_registered`, `staging_models_registered`, `production_models_registered`, `promotions_to_staging`, `promotions_to_production`, `active_production_models`
- **Metadata**: `updated_at`

Contract rules:

- Primary key is `progress_date`.
- Table is allowed to be sparse for dates with no ML or pipeline events.

## Query guidance for dashboards

- Always filter by time window first (`match_date`, `last_seen_at`).
- Select only required columns; avoid `SELECT *`.
- Avoid joining serving tables back to `core` or `features` in dashboard paths.
- Cache frequent slices where the BI tool supports cached extracts.
- Use Superset setup in `infra/superset/README.md` for local dashboard rollout.

Starter query pack:

- `infra/trino/queries/dashboard/dashboard_player_profile_and_momentum.sql`
- `infra/trino/queries/dashboard/dashboard_map_competitiveness_and_pace.sql`
- `infra/trino/queries/dashboard/dashboard_chat_behaviour_and_tilt_risk.sql`
- `infra/trino/queries/dashboard/dashboard_player_match_deep_dive.sql`
- `infra/trino/queries/dashboard/dashboard_ml_progress_and_registry.sql`

## Change management

- **Non-breaking changes**: add nullable columns, add derived metrics that do not alter existing semantics.
- **Breaking changes**: rename/drop columns, change grain or key semantics, or materially change metric definitions.

For breaking changes, publish a versioned table and run both versions during a deprecation window.

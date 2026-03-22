# Dashboard dataset contract for serving tables

This document defines stable contracts for dashboard queries. Dashboards should read only from `serving_*` tables.

## Scope

Contract tables:

- `tf2.default.serving_player_profiles`
- `tf2.default.serving_map_overview_daily`

Non-contract tables (`core`, `features`) may evolve more quickly and should not be direct dashboard dependencies.

## Freshness and publish policy

- Refresh cadence: daily incremental refresh (or more often if required).
- Publish gate: `19_data_quality_checks.sql` must be all `PASS`.
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
- Breaking schema updates require versioned table rollout.

## Query guidance for dashboards

- Always filter by time window first (`match_date`, `last_seen_at`).
- Select only required columns; avoid `SELECT *`.
- Avoid joining serving tables back to `core` or `features` in dashboard paths.
- Cache frequent slices where the BI tool supports cached extracts.
- Use Superset setup in `infra/superset/README.md` for local dashboard rollout.

Starter query pack:

- `infra/trino/queries/21_dashboard_player_profile_and_momentum.sql`
- `infra/trino/queries/22_dashboard_map_competitiveness_and_pace.sql`
- `infra/trino/queries/23_dashboard_chat_behaviour_and_tilt_risk.sql`

## Change management

- **Non-breaking changes**: add nullable columns, add derived metrics that do not alter existing semantics.
- **Breaking changes**: rename/drop columns, change grain or key semantics, or materially change metric definitions.

For breaking changes, publish a versioned table and run both versions during a deprecation window.

# Baseline model feature list and labels

This baseline spec maps model inputs to `features_*` sources and defines labels for V1 training.

## Snapshot policy

- Training datasets must be materialised from a fixed Iceberg snapshot/timepoint.
- Each model run must record snapshot timestamp, source table versions, and training code version.

## Source tables

- `tf2.default.features_player_match`
- `tf2.default.features_player_recent_form`

## Model 1: win probability

Prediction target:

- probability that player-team record wins (`won_game = 1`) at match level.

Label definition:

- `label_win = CAST(won_game AS INTEGER)` from `features_player_match`.

Recommended features:

- **Match context**: `map`, `team`, `duration_seconds`, `team_score`, `opponent_score`, `score_delta`
- **Player production**: `kills`, `assists`, `deaths`, `damage_dealt`, `healing_done`, `ubers_used`
- **Relative contribution**: `kill_share_of_team`, `damage_share_of_team`, `healing_share_of_team`, `impact_index`
- **Pace/efficiency**: `damage_per_minute`, `kda_ratio`
- **Recent form**: `rolling_5_avg_kills`, `rolling_10_avg_damage`, `rolling_10_avg_impact`, `rolling_10_kda_ratio`, `rolling_10_win_rate`, `form_delta_kills`, `form_delta_damage`, `form_delta_impact`, `momentum_label`

Exclusions:

- do not include post-outcome leakage features beyond match-observed inputs.

## Model 2: player impact percentile

Prediction target:

- percentile rank of player impact at match level.

Label definition:

- `label_impact_percentile = NTILE(100) OVER (PARTITION BY match_date ORDER BY impact_index)`.

Recommended features:

- **Production and contribution**: `kills`, `assists`, `deaths`, `damage_dealt`, `healing_done`, `ubers_used`, `kill_share_of_team`, `damage_share_of_team`, `healing_share_of_team`
- **Form trajectory**: `rolling_10_avg_impact`, `career_avg_impact`, `career_avg_damage`, `career_avg_kills`, `form_delta_impact`, `games_played_to_date`
- **Style context**: `classes_played_count`, `map`, `team`

## Model 3: toxicity/tilt classifier

Prediction target:

- binary risk of tilt/toxic behaviour during a match.

Label definition:

- baseline: `label_tilt = CAST(possible_tilt_label AS INTEGER)`.
- follow-up improvement: replace or blend with human-reviewed labels once available.

Recommended features:

- **Chat intensity**: `chat_messages`, `avg_message_length`, `all_caps_messages`, `intense_punctuation_messages`, `negative_lexicon_hits`, `negative_chat_ratio`
- **Gameplay stress proxies**: `deaths`, `won_game`, `score_delta`, `impact_index`
- **Behavioural history**: `rolling_10_negative_chat_ratio`, `rolling_10_win_rate`, `form_delta_impact`, `momentum_label`

Class-balance guidance:

- apply stratified sampling or class weights if positives are sparse.

## Shared preprocessing guidance

- Split by time (train on older periods, validate on newer periods).
- Encode categorical fields (`map`, `team`, `momentum_label`) consistently.
- Impute nulls explicitly and persist imputation strategy with model metadata.
- Standardise/normalise numerical features where model family benefits.

## Offline evaluation minimum

For each baseline model, publish:

- precision, recall, F1
- ROC-AUC / PR-AUC where applicable
- calibration plot notes (especially for win probability)
- feature importance summary and known caveats

## Readiness check before training

Run this before ML pipeline snapshot materialisation and training:

```bash
infra/trino/queries/run_ml_readiness_check.sh
```

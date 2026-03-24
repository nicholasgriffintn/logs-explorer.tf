# Apache Atlas implementation plan

## Problem

We have useful table contracts and run metadata, but no central catalogue for technical metadata, ownership, glossary terms, and lineage across Spark and Trino workflows.

## What Atlas provides

- central metadata catalogue for data assets
- end-to-end lineage between source, transform, and serving assets
- business glossary and classification model
- API-driven metadata integration for governance and auditing

## Scope

In scope:

- metadata registration for `core`, `features_*`, `serving_*`, and ML tables
- process lineage for Spark pipeline steps
- ownership and glossary definitions

Out of scope for V1:

- automatic column-level lineage for every SQL expression
- policy enforcement (Ranger handles that)
- replacing existing docs and contracts

## Target metadata model

Core entity types:

- DataSet entities for Iceberg tables in `tf2.default`
- Process entities for pipeline steps:
  - `features_refresh`
  - `serving_player_profiles_refresh`
  - `serving_map_overview_daily_refresh`
  - `serving_player_match_deep_dive_refresh`
  - `ml_training_snapshot_refresh`
  - `serving_ml_progress_refresh`
- Glossary terms for domain concepts:
  - player
  - match
  - momentum
  - behaviour risk
  - model stage

## Implementation phases

## Phase 0: platform decision

- confirm Atlas runtime choice for production
- define where Atlas dependencies run and who owns them
- define metadata ownership model in engineering

Deliverables:

- architecture decision record for Atlas deployment mode
- ownership matrix for entity stewardship

## Phase 1: bootstrap Atlas in non-production

- deploy Atlas and required dependencies in a test environment
- configure authentication and admin users
- verify API access and metadata write/read workflow

Deliverables:

- `infra/atlas/README.md`
- baseline health checks and startup scripts

## Phase 2: metadata ingestion for table inventory

- build a metadata sync script to register tables from Trino `information_schema`
- attach attributes:
  - schema
  - owner
  - freshness expectation
  - contract status
- run sync after each schema change

Deliverables:

- `infra/atlas/sync_tables.py` (or equivalent)
- scheduled metadata sync job

## Phase 3: process lineage capture

- register Spark pipeline steps as Process entities
- link input/output datasets for each step
- include run metadata references from `ops_pipeline_runs`

Deliverables:

- lineage graph from `logs/summaries/messages` to `serving_*` and ML tables
- run-to-lineage traceability from operations logs

## Phase 4: glossary and classification rollout

- define glossary terms with clear definitions
- define table-level tags:
  - public analytics
  - restricted operations
  - model governance
- apply tags to key serving and ML tables

Deliverables:

- v1 glossary published
- table classification baseline complete

## Phase 5: operational hardening

- add monitoring for sync job failures
- add drift checks for missing lineage edges
- define quarterly metadata quality review

Deliverables:

- metadata quality scorecard
- governance runbook for Atlas outages

## Integration points with current stack

- Trino provides discoverable table inventory
- Spark pipeline step names provide stable process identifiers
- existing docs remain source narrative; Atlas becomes source system for machine-readable metadata

## Risks and mitigations

Risk:

- Atlas stack complexity increases maintenance burden.

Mitigation:

- phase rollout and keep V1 to table-level lineage

Risk:

- stale metadata if sync fails silently.

Mitigation:

- enforce sync alerts and weekly completeness checks

## Success criteria

- 100 percent of production tables in Atlas with owner and domain tags
- lineage coverage for all pipeline step outputs
- glossary terms used in dashboard and model documentation
- metadata sync failures detected within 15 minutes

## Estimated effort

- Phase 0-1: 1-2 weeks
- Phase 2-3: 1-2 weeks
- Phase 4-5: 1 week
- total: ~3-5 weeks elapsed with one engineer and stakeholder support

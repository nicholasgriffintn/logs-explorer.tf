# Apache Pinot vs Apache Druid implementation plan

## Problem

Trino on Iceberg is currently the default serving path and should remain default while it meets dashboard SLOs. If query concurrency and latency targets degrade, we need a clear plan for a dedicated low-latency OLAP serving engine.

## What Pinot and Druid provide

Apache Pinot:

- very low-latency analytics for high-cardinality dimensions
- strong performance for filtered aggregations and point lookups at scale
- near-real-time ingestion support when needed

Apache Druid:

- strong time-series and roll-up analytics performance
- mature ingestion and segment management model
- near-real-time and batch ingestion patterns

## Recommendation

Keep Trino as default until decision gates are triggered.

If a second serving engine is needed, test both with the same benchmark pack and choose using measured results. Do not pre-commit to either engine without workload evidence.

## Decision gates

Start PoC only if one or more conditions persist for two consecutive weeks:

- dashboard query P95 exceeds 3 seconds on critical slices
- concurrent dashboard users exceed Trino/Spark tuning headroom
- serving-table scans become cost-inefficient despite Iceberg maintenance

## Candidate workloads for PoC

- player profile point lookup
- map competitiveness roll-up over recent windows
- chat behaviour risk hot-spot roll-up
- player deep-dive by `steamid` + `match_date`

Use existing benchmark source:

- `infra/trino/queries/quality/serving_query_performance_benchmark.sql`

## PoC architecture

- keep Spark as transformation engine and source of truth for serving tables
- export selected `serving_*` tables to engine-specific ingestion
- keep Superset as BI layer and compare datasource performance side by side
- keep Trino path active as fallback

## Implementation phases

## Phase 0: benchmark baseline

- capture current Trino benchmark metrics with representative concurrency
- define fixed test dataset window and query parameter set
- define pass/fail criteria for candidate engines

Deliverables:

- benchmark baseline report
- acceptance thresholds

## Phase 1: Pinot and Druid dev clusters

- deploy non-production Pinot cluster
- deploy non-production Druid cluster
- ingest identical subset of `serving_*` tables into both

Deliverables:

- reproducible infrastructure setup docs
- validated ingestion pipelines for both engines

## Phase 2: query parity

- re-implement benchmark queries for Pinot and Druid semantics
- validate metric parity against Trino outputs
- document any semantic drift and acceptable tolerances

Deliverables:

- query parity report
- correctness sign-off

## Phase 3: performance and cost tests

- run controlled load tests for all three paths
- measure:
  - P50/P95 latency
  - throughput at target concurrency
  - compute and storage cost
  - operational overhead

Deliverables:

- comparative decision matrix
- recommendation memo

## Phase 4: production rollout for chosen engine

- expose chosen engine to Superset for selected dashboards
- run dual-read period with Trino fallback
- switch dashboard traffic gradually

Deliverables:

- cutover runbook
- rollback runbook

## Selection criteria

- latency under target at required concurrency
- metric parity with existing serving contracts
- manageable operational complexity for team size
- clear cost advantage at projected scale

## Risks and mitigations

Risk:

- dual-serving planes increase operational burden.

Mitigation:

- limit new engine to only high-pressure dashboards

Risk:

- semantic drift between query engines.

Mitigation:

- enforce parity tests against serving contract outputs

Risk:

- premature adoption with no sustained need.

Mitigation:

- strict decision gates before production commitment

## Success criteria

- selected engine improves critical query P95 by at least 40 percent
- dashboard correctness parity above 99.5 percent on validated metrics
- no critical incidents during dual-read cutover window
- clear cost model for 6-12 month growth projection

## Estimated effort

- Phase 0-1: 1-2 weeks
- Phase 2-3: 2-3 weeks
- Phase 4: 1-2 weeks
- total: ~4-7 weeks elapsed for full evaluation and controlled rollout

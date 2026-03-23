# Serving query performance tuning

Dashboard reads were at risk of avoidable full-table work on every request, especially in the player profile view where percentile ranks were calculated with window functions at query time.
That pattern scales poorly as player history grows and makes the P95 under 3 seconds target harder to hold.

## What we changed

- Rewrote `21_dashboard_player_profile_and_momentum.sql` to compute target-player percentiles using a single population scan instead of full-table window ranking.
- Kept dashboard dependencies on `serving_*` tables only, so contract boundaries remain unchanged.
- Added `24_serving_query_performance_benchmark.sql` as a repeatable benchmark pack using `EXPLAIN ANALYZE`.

## How to benchmark

Run:

```bash
docker exec -i tf2-trino trino < infra/trino/queries/24_serving_query_performance_benchmark.sql
```

For each benchmark query:

- run at least 5 times
- record the `EXPLAIN ANALYZE` wall time
- record scanned input rows and data size
- compute P95 from the recorded wall times

## Pass criteria

- `21_*` player profile point lookup: P95 under 3 seconds.
- `22_*` map competitiveness rollup (30-day slice): P95 under 3 seconds.
- `23_*` tilt hotspot rollup (14-day slice): P95 under 3 seconds.

If any benchmark misses target for two consecutive weeks, escalate to the platform decision gate for additional runtime components.

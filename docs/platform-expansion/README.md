# Platform expansion roadmap

This roadmap defines governance and serving expansion on top of the current Airflow-orchestrated platform baseline.

Detailed plans:

- `docs/platform-expansion/airflow-implementation-plan.md` (implemented orchestration architecture)
- `docs/platform-expansion/atlas-implementation-plan.md`
- `docs/platform-expansion/ranger-implementation-plan.md`
- `docs/platform-expansion/pinot-vs-druid-implementation-plan.md`

Execution principles:

- keep Spark as processing owner for `features_*`, `serving_*`, and ML tables
- keep Trino as default query plane unless decision gates say otherwise

# Platform expansion roadmap

This roadmap defines how we add orchestration, governance, and optional low-latency serving.

Detailed plans:

- `docs/platform-expansion/airflow-implementation-plan.md`
- `docs/platform-expansion/atlas-implementation-plan.md`
- `docs/platform-expansion/ranger-implementation-plan.md`
- `docs/platform-expansion/pinot-vs-druid-implementation-plan.md`

Execution principles:

- keep Spark as processing owner for `features_*`, `serving_*`, and ML tables
- keep Trino as default query plane unless decision gates say otherwise

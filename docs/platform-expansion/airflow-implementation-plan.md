# Apache Airflow implementation plan

## Problem

Current refresh and quality workflows are script-driven. That keeps the stack simple, but it limits scheduling control, failure handling, and operational visibility as pipeline count grows.

## What Airflow provides

- reliable orchestration for feature-serving, ML, quality, and maintenance pipelines
- retries, dependency management, and backfill controls
- central run history and task-level observability
- SLA and failure alerting
- clearer separation between pipeline definition and runtime execution

## Scope

In scope:

- orchestrating existing commands under `infra/spark` and `infra/trino/queries`
- scheduling and alerting
- run metadata publication

## Target architecture

- Airflow Scheduler + Webserver + Worker(s) with a shared metadata database
- DAG tasks execute existing project commands:
  - `infra/spark/run_feature_pipeline.sh incremental`
  - `infra/spark/run_ml_pipeline.sh incremental`
  - `docker exec -i tf2-trino trino < infra/trino/queries/quality/data_quality_checks.sql`
  - `infra/trino/queries/ml/run_ml_readiness_check.sh`
  - `infra/trino/queries/ops/run_iceberg_maintenance.sh`

## DAG design

Define DAGs:

- `tf2_feature_serving_daily`
- `tf2_ml_daily_or_weekly`
- `tf2_iceberg_maintenance_weekly`
- `tf2_backfill_manual`

Define core task sequence for feature-serving DAG:

- run Spark feature-serving refresh
- run data quality checks
- publish success/failure event

Define core task sequence for ML DAG:

- run Spark ML refresh
- run ML readiness checks
- optional baseline training trigger

## Implementation phases

## Phase 0: prerequisites

- setup dockerized Airflow environment
- create Airflow metadata database
- create secret management pattern for Spark/R2/Trino credentials

Deliverables:

- `infra/airflow/README.md`
- environment variable contract for Airflow workers
- runbook for Airflow component failures

## Phase 1: local Airflow foundation

- add `infra/airflow/docker-compose.yml`
- add `infra/airflow/dags` with placeholder DAGs
- run one no-op DAG to validate scheduler/executor health

Deliverables:

- local Airflow up/down commands
- first DAG parse and execute success

## Phase 2: production DAGs for existing workflows

- implement feature-serving DAG from existing scripts
- implement ML DAG from existing scripts
- set retry and timeout policy per task
- set task concurrency and DAG concurrency controls

Recommended default policy:

- retries: `2`
- retry delay: `10m`
- task timeout: `90m` for Spark pipeline tasks

Deliverables:

- Airflow-triggered runs replacing cron/manual invocation
- Airflow run IDs linked to `ops_pipeline_runs.run_id` in logs

## Phase 3: alerting and operations

- add SLA miss alerts and hard failure alerts
- add dashboards for DAG success rate and mean duration
- document rerun and backfill operations

Deliverables:

- operations playbook for failed tasks
- alert routing verified in test incident

## Phase 4: hardening

- enable deployment pipeline for DAG changes
- add DAG unit tests for task graph validation
- lock production change windows for orchestration updates

Deliverables:

- CI checks for DAG syntax and import health
- release checklist for Airflow changes

## Security and access

- store credentials in Airflow connections/secrets backend, not in DAG code
- use least-privilege credentials for Spark/Trino operations
- lock Airflow UI behind SSO or strong auth

## Risks and mitigations

Risk:

- Airflow adds operational overhead.

Mitigation:

- start with only existing scripts and minimal operators

Risk:

- duplicate scheduling during migration.

Mitigation:

- run in shadow mode first, then cut over one workflow at a time

## Success criteria

- 95 percent+ scheduled DAG success over 30 days
- mean time to detect pipeline failure under 5 minutes
- no manual daily triggers required for normal operation
- runbooks used successfully for at least one controlled failure test

## Estimated effort

- Phase 0-1: 3-5 engineering days
- Phase 2: 4-7 engineering days
- Phase 3-4: 3-5 engineering days
- total: ~2-3 weeks elapsed with one engineer

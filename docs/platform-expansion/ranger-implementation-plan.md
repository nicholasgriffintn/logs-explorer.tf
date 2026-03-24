# Apache Ranger implementation plan

## Problem

Access control is currently implicit and environment-based. As more users and services query Trino and consume serving tables, we need explicit policy management, auditability, and least-privilege controls.

## What Ranger provides

- centralised policy management for data access
- fine-grained permissions by user/group/schema/table/column
- audit logs for who accessed what and when
- optional tag-based policy alignment with Atlas classifications

## Scope

In scope:

- Trino query-layer authorisation policy
- role and group model for analytics and ML workflows
- access audit logging

Out of scope for V1:

- row-level masking for every consumer path
- object-store IAM replacement
- Spark-native fine-grained policy enforcement

## Target policy model

Principal groups:

- `platform_admin`
- `data_engineer`
- `analyst_readonly`
- `ml_engineer`
- `service_account_pipeline`

Policy boundaries:

- `core` source tables: restricted read access
- `features_*` tables: engineering and ML access
- `serving_*` tables: broader read access for BI and product analytics
- `ml_model_*` and training snapshot tables: restricted write and promotion access

## Implementation phases

## Phase 0: policy baseline design

- define user/group mapping from identity provider
- define minimum permission set per persona
- define break-glass administrative process

Deliverables:

- policy matrix document
- approved group mapping

## Phase 1: non-production Ranger deployment

- deploy Ranger admin and database
- configure authentication and admin access
- integrate Trino with Ranger authorisation plugin/configuration

Deliverables:

- `infra/ranger/README.md`
- non-production policy authoring workflow

## Phase 2: enforce read policies in shadow mode

- replicate current effective access as explicit policies
- enable audit-only checks before strict deny
- compare actual query behaviour with expected policy model

Deliverables:

- shadow validation report
- zero unexpected deny events before enforcement cutover

## Phase 3: production enforcement

- enable strict policy enforcement for Trino
- lock direct production query access to approved groups
- route all policy changes through review workflow

Deliverables:

- production policy baseline applied
- policy change process and approver list

## Phase 4: governance integration

- align Ranger policies to Atlas tags where useful
- add periodic access recertification
- add audit log retention and review schedule

Deliverables:

- tag-driven policy prototype
- quarterly access review process

## Security and operations controls

- deny-by-default for new schemas/tables
- read-only defaults for analysts
- strong separation of policy admin and data admin roles
- audit export retained for incident response

## Risks and mitigations

Risk:

- policy misconfiguration can block critical workloads.

Mitigation:

- staged rollout with shadow mode and rollback policy bundles

Risk:

- unmanaged policy sprawl over time.

Mitigation:

- policy naming standards and monthly policy hygiene review

## Success criteria

- 100 percent of Trino production access routed through Ranger policies
- unauthorised access attempts visible in audit logs
- no manual ad-hoc permission grants outside approved workflow
- less than 1 percent policy-change rollback rate after first quarter

## Estimated effort

- Phase 0-1: 1-2 weeks
- Phase 2-3: 1-2 weeks
- Phase 4: 1 week
- total: ~3-5 weeks elapsed with platform and security support

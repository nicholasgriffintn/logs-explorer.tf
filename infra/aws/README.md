# AWS platform CDK

Deploys:

- Trino on EMR
- Superset on ECS Fargate
- Marquez (OpenLineage API + UI) on ECS Fargate
- Airflow on MWAA

## Quick start

```bash
pnpm install
pnpm --filter @logs-explorer/aws build
pnpm --filter @logs-explorer/aws synth
pnpm --filter @logs-explorer/aws deploy
```

## Key context values

Set in `infra/aws/cdk.json` or pass via `-c`:

- `environment`
- `projectName`
- `emrReleaseLabel`
- `trinoInstanceType`
- `trinoCoreInstanceCount`
- `metadataDbInstanceType`
- `metadataDbAllocatedStorageGiB`
- `metadataDbMaxAllocatedStorageGiB`
- `marquezApiDesiredCount`
- `marquezApiMinCapacity`
- `marquezApiMaxCapacity`
- `enableMwaa`
- `mwaaAirflowVersion`
- `mwaaEnvironmentClass`
- `mwaaMinWorkers`
- `mwaaMaxWorkers`
- `mwaaSchedulers`
- `publicUis`

Example:

```bash
pnpm --filter @logs-explorer/awsexec cdk deploy \
  -c environment=prod \
  -c publicUis=true
```

For a low-cost PoC profile, keep `enableMwaa=false` (default). To enable Airflow when needed:

```bash
pnpm --filter @logs-explorer/aws deploy -- \
  -c enableMwaa=true \
  -c mwaaEnvironmentClass=mw1.micro \
  -c mwaaMinWorkers=1 \
  -c mwaaMaxWorkers=1 \
  -c mwaaSchedulers=1
```

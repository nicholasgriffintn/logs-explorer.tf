#!/usr/bin/env node
import "source-map-support/register.js";
import * as cdk from "aws-cdk-lib";

import { AwsPlatformStack } from "../lib/aws-platform-stack.js";

const app = new cdk.App();
const environmentName = app.node.tryGetContext("environment") ?? "dev";
const projectName = app.node.tryGetContext("projectName") ?? "tf2-logs-explorer";

new AwsPlatformStack(app, "LogsExplorerAwsPlatformStack", {
  description:
    "AWS CDK platform stack for Trino (EMR), Superset (ECS), OpenLineage/Marquez (ECS), and Airflow (MWAA)",
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
  tags: {
    Project: projectName,
    Environment: environmentName,
    ManagedBy: "cdk",
  },
});

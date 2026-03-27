import * as path from "node:path";
import { fileURLToPath } from "node:url";

import * as cdk from "aws-cdk-lib";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as ecs from "aws-cdk-lib/aws-ecs";
import * as ecsPatterns from "aws-cdk-lib/aws-ecs-patterns";
import * as emr from "aws-cdk-lib/aws-emr";
import * as iam from "aws-cdk-lib/aws-iam";
import * as kms from "aws-cdk-lib/aws-kms";
import * as logs from "aws-cdk-lib/aws-logs";
import * as mwaa from "aws-cdk-lib/aws-mwaa";
import * as rds from "aws-cdk-lib/aws-rds";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3deploy from "aws-cdk-lib/aws-s3-deployment";
import * as secretsmanager from "aws-cdk-lib/aws-secretsmanager";
import * as servicediscovery from "aws-cdk-lib/aws-servicediscovery";
import { Construct } from "constructs";

const MODULE_DIR = path.dirname(fileURLToPath(import.meta.url));

export class AwsPlatformStack extends cdk.Stack {
  public constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const environmentName = (this.node.tryGetContext("environment") as string) ?? "dev";
    const projectName = (this.node.tryGetContext("projectName") as string) ?? "logs-explorer";
    const emrReleaseLabel = (this.node.tryGetContext("emrReleaseLabel") as string) ?? "emr-7.12.0";
    const mwaaAirflowVersion = (this.node.tryGetContext("mwaaAirflowVersion") as string) ?? "3.0.6";
    const mwaaEnvironmentClass =
      (this.node.tryGetContext("mwaaEnvironmentClass") as string) ?? "mw1.micro";
    const enableMwaaContext = this.node.tryGetContext("enableMwaa");
    const enableMwaa = enableMwaaContext === true || enableMwaaContext === "true";
    const trinoCoreInstanceCount = Number(this.node.tryGetContext("trinoCoreInstanceCount") ?? 1);
    const trinoInstanceType =
      (this.node.tryGetContext("trinoInstanceType") as string) ?? "m6i.xlarge";
    const metadataDbInstanceType =
      (this.node.tryGetContext("metadataDbInstanceType") as string) ?? "t4g.micro";
    const metadataDbAllocatedStorageGiB = Number(
      this.node.tryGetContext("metadataDbAllocatedStorageGiB") ?? 20,
    );
    const metadataDbMaxAllocatedStorageGiB = Number(
      this.node.tryGetContext("metadataDbMaxAllocatedStorageGiB") ?? 100,
    );
    const publicUisContext = this.node.tryGetContext("publicUis");
    const publicUis = publicUisContext === true || publicUisContext === "true";
    const marquezApiImage =
      (this.node.tryGetContext("marquezApiImage") as string) ?? "marquezproject/marquez:0.51.1";
    const marquezWebImage =
      (this.node.tryGetContext("marquezWebImage") as string) ?? "marquezproject/marquez-web:0.51.1";
    const marquezApiDesiredCount = Number(this.node.tryGetContext("marquezApiDesiredCount") ?? 1);
    const marquezApiMinCapacity = Number(this.node.tryGetContext("marquezApiMinCapacity") ?? 1);
    const marquezApiMaxCapacity = Number(this.node.tryGetContext("marquezApiMaxCapacity") ?? 2);
    const mwaaMinWorkers = Number(this.node.tryGetContext("mwaaMinWorkers") ?? 1);
    const mwaaMaxWorkers = Number(this.node.tryGetContext("mwaaMaxWorkers") ?? 1);
    const defaultMwaaSchedulers = mwaaEnvironmentClass === "mw1.micro" ? 1 : 2;
    const mwaaSchedulers = Number(
      this.node.tryGetContext("mwaaSchedulers") ?? defaultMwaaSchedulers,
    );

    const namePrefix = `${projectName}-${environmentName}`;
    const packageRootPath = path.resolve(MODULE_DIR, "..", "..");
    const repoRootPath = path.resolve(packageRootPath, "..", "..");

    if (!Number.isInteger(trinoCoreInstanceCount) || trinoCoreInstanceCount < 1) {
      throw new Error("trinoCoreInstanceCount must be an integer greater than or equal to 1.");
    }
    if (!Number.isInteger(metadataDbAllocatedStorageGiB) || metadataDbAllocatedStorageGiB < 20) {
      throw new Error(
        "metadataDbAllocatedStorageGiB must be an integer greater than or equal to 20.",
      );
    }
    if (
      !Number.isInteger(metadataDbMaxAllocatedStorageGiB) ||
      metadataDbMaxAllocatedStorageGiB < metadataDbAllocatedStorageGiB
    ) {
      throw new Error(
        "metadataDbMaxAllocatedStorageGiB must be an integer greater than or equal to metadataDbAllocatedStorageGiB.",
      );
    }
    if (!Number.isInteger(marquezApiDesiredCount) || marquezApiDesiredCount < 1) {
      throw new Error("marquezApiDesiredCount must be an integer greater than or equal to 1.");
    }
    if (!Number.isInteger(marquezApiMinCapacity) || marquezApiMinCapacity < 1) {
      throw new Error("marquezApiMinCapacity must be an integer greater than or equal to 1.");
    }
    if (!Number.isInteger(marquezApiMaxCapacity) || marquezApiMaxCapacity < marquezApiMinCapacity) {
      throw new Error(
        "marquezApiMaxCapacity must be an integer greater than or equal to marquezApiMinCapacity.",
      );
    }
    if (
      marquezApiDesiredCount < marquezApiMinCapacity ||
      marquezApiDesiredCount > marquezApiMaxCapacity
    ) {
      throw new Error(
        "marquezApiDesiredCount must be between marquezApiMinCapacity and marquezApiMaxCapacity.",
      );
    }
    if (enableMwaa) {
      if (!Number.isInteger(mwaaMinWorkers) || mwaaMinWorkers < 1) {
        throw new Error("mwaaMinWorkers must be an integer greater than or equal to 1.");
      }
      if (!Number.isInteger(mwaaMaxWorkers) || mwaaMaxWorkers < mwaaMinWorkers) {
        throw new Error(
          "mwaaMaxWorkers must be an integer greater than or equal to mwaaMinWorkers.",
        );
      }
      if (!Number.isInteger(mwaaSchedulers) || mwaaSchedulers < 1) {
        throw new Error("mwaaSchedulers must be an integer greater than or equal to 1.");
      }
      if (mwaaEnvironmentClass === "mw1.micro" && mwaaSchedulers !== 1) {
        throw new Error("mwaaSchedulers must be 1 when mwaaEnvironmentClass is mw1.micro.");
      }
      if (mwaaEnvironmentClass !== "mw1.micro" && mwaaSchedulers < 2) {
        throw new Error(
          "mwaaSchedulers must be greater than or equal to 2 for environment classes larger than mw1.micro.",
        );
      }
    }

    const sharedKey = new kms.Key(this, "SharedPlatformKey", {
      alias: `alias/${namePrefix}-platform`,
      enableKeyRotation: true,
      description: "Shared KMS key for buckets and platform secrets.",
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    const vpc = new ec2.Vpc(this, "PlatformVpc", {
      maxAzs: 2,
      natGateways: 1,
      subnetConfiguration: [
        {
          name: "public",
          subnetType: ec2.SubnetType.PUBLIC,
          cidrMask: 24,
        },
        {
          name: "app",
          subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
          cidrMask: 24,
        },
        {
          name: "data",
          subnetType: ec2.SubnetType.PRIVATE_ISOLATED,
          cidrMask: 24,
        },
      ],
    });

    vpc.addGatewayEndpoint("S3Endpoint", {
      service: ec2.GatewayVpcEndpointAwsService.S3,
      subnets: [
        { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
        { subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
      ],
    });

    const serviceNamespace = new servicediscovery.PrivateDnsNamespace(this, "ServicesNamespace", {
      vpc,
      name: `${environmentName}.platform.local`,
    });

    const appSecurityGroup = new ec2.SecurityGroup(this, "AppSecurityGroup", {
      vpc,
      description: "ECS workloads for Superset and OpenLineage",
      allowAllOutbound: true,
    });

    appSecurityGroup.addIngressRule(
      appSecurityGroup,
      ec2.Port.allTcp(),
      "Allow service-to-service traffic within ECS application tier.",
    );

    const metadataDbSecurityGroup = new ec2.SecurityGroup(this, "MetadataDbSecurityGroup", {
      vpc,
      description: "PostgreSQL metadata database for Superset and Marquez.",
      allowAllOutbound: true,
    });

    metadataDbSecurityGroup.addIngressRule(
      appSecurityGroup,
      ec2.Port.tcp(5432),
      "Allow ECS tasks to connect to metadata DB.",
    );

    let mwaaSecurityGroup: ec2.SecurityGroup | undefined;
    if (enableMwaa) {
      mwaaSecurityGroup = new ec2.SecurityGroup(this, "MwaaSecurityGroup", {
        vpc,
        description: "Security group for Amazon MWAA workers and webserver.",
        allowAllOutbound: true,
      });

      // MWAA requires internal component communication over the same security group.
      mwaaSecurityGroup.addIngressRule(
        mwaaSecurityGroup,
        ec2.Port.allTraffic(),
        "Allow MWAA internal component traffic.",
      );

      appSecurityGroup.addIngressRule(
        mwaaSecurityGroup,
        ec2.Port.tcp(5000),
        "Allow MWAA OpenLineage events to reach Marquez API.",
      );
    }

    const emrMasterSecurityGroup = new ec2.SecurityGroup(this, "EmrMasterSecurityGroup", {
      vpc,
      description: "EMR primary node security group.",
      allowAllOutbound: true,
    });

    const emrCoreSecurityGroup = new ec2.SecurityGroup(this, "EmrCoreSecurityGroup", {
      vpc,
      description: "EMR core node security group.",
      allowAllOutbound: true,
    });

    const emrServiceAccessSecurityGroup = new ec2.SecurityGroup(
      this,
      "EmrServiceAccessSecurityGroup",
      {
        vpc,
        description: "EMR service access security group for private subnet clusters.",
        allowAllOutbound: true,
      },
    );

    emrMasterSecurityGroup.addIngressRule(
      emrMasterSecurityGroup,
      ec2.Port.allTraffic(),
      "Allow EMR primary self-traffic.",
    );
    emrCoreSecurityGroup.addIngressRule(
      emrCoreSecurityGroup,
      ec2.Port.allTraffic(),
      "Allow EMR core self-traffic.",
    );
    emrMasterSecurityGroup.addIngressRule(
      emrCoreSecurityGroup,
      ec2.Port.allTraffic(),
      "Allow EMR core-to-primary communication.",
    );
    emrCoreSecurityGroup.addIngressRule(
      emrMasterSecurityGroup,
      ec2.Port.allTraffic(),
      "Allow EMR primary-to-core communication.",
    );
    emrMasterSecurityGroup.addIngressRule(
      emrServiceAccessSecurityGroup,
      ec2.Port.allTraffic(),
      "Allow EMR service access traffic to primary node.",
    );
    emrCoreSecurityGroup.addIngressRule(
      emrServiceAccessSecurityGroup,
      ec2.Port.allTraffic(),
      "Allow EMR service access traffic to core nodes.",
    );

    emrMasterSecurityGroup.addIngressRule(
      appSecurityGroup,
      ec2.Port.tcp(8889),
      "Allow Superset and internal services to query Trino.",
    );
    if (mwaaSecurityGroup) {
      emrMasterSecurityGroup.addIngressRule(
        mwaaSecurityGroup,
        ec2.Port.tcp(8889),
        "Allow MWAA DAGs to query Trino.",
      );
    }

    const emrLogsBucket = new s3.Bucket(this, "EmrLogsBucket", {
      encryption: s3.BucketEncryption.KMS,
      encryptionKey: sharedKey,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      enforceSSL: true,
      versioned: true,
      lifecycleRules: [{ expiration: cdk.Duration.days(365) }],
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    let mwaaBucket: s3.Bucket | undefined;
    if (enableMwaa) {
      mwaaBucket = new s3.Bucket(this, "MwaaAssetsBucket", {
        encryption: s3.BucketEncryption.KMS,
        encryptionKey: sharedKey,
        blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
        enforceSSL: true,
        versioned: true,
        removalPolicy: cdk.RemovalPolicy.RETAIN,
      });
    }

    const metadataDb = new rds.DatabaseInstance(this, "MetadataDb", {
      engine: rds.DatabaseInstanceEngine.postgres({
        version: rds.PostgresEngineVersion.of("17.8", "17"),
      }),
      credentials: rds.Credentials.fromGeneratedSecret("platform_admin"),
      instanceType: new ec2.InstanceType(`db.${metadataDbInstanceType}`.replace(/^db\./, "")),
      vpc,
      vpcSubnets: {
        subnetType: ec2.SubnetType.PRIVATE_ISOLATED,
      },
      securityGroups: [metadataDbSecurityGroup],
      databaseName: "platform",
      allocatedStorage: metadataDbAllocatedStorageGiB,
      maxAllocatedStorage: metadataDbMaxAllocatedStorageGiB,
      multiAz: false,
      storageEncrypted: true,
      storageEncryptionKey: sharedKey,
      backupRetention: cdk.Duration.days(7),
      deletionProtection: true,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      cloudwatchLogsExports: ["postgresql"],
      cloudwatchLogsRetention: logs.RetentionDays.ONE_MONTH,
    });

    const metadataDbSecret = metadataDb.secret;
    if (!metadataDbSecret) {
      throw new Error("RDS metadata secret must exist for ECS and MWAA wiring.");
    }

    const supersetAdminSecret = new secretsmanager.Secret(this, "SupersetAdminSecret", {
      secretName: `${namePrefix}/superset/admin`,
      encryptionKey: sharedKey,
      generateSecretString: {
        secretStringTemplate: JSON.stringify({
          username: "admin",
          firstname: "TF2",
          lastname: "Admin",
          email: "admin@example.com",
        }),
        generateStringKey: "password",
        excludePunctuation: true,
        passwordLength: 24,
      },
    });

    const supersetSecretKey = new secretsmanager.Secret(this, "SupersetSecretKey", {
      secretName: `${namePrefix}/superset/secret-key`,
      encryptionKey: sharedKey,
      generateSecretString: {
        excludePunctuation: true,
        passwordLength: 64,
      },
    });

    const emrServiceRole = new iam.Role(this, "EmrServiceRole", {
      assumedBy: new iam.ServicePrincipal("elasticmapreduce.amazonaws.com"),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AmazonEMRServicePolicy_v2"),
      ],
    });

    const emrEc2Role = new iam.Role(this, "EmrEc2Role", {
      assumedBy: new iam.ServicePrincipal("ec2.amazonaws.com"),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AmazonElasticMapReduceforEC2Role"),
        iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonSSMManagedInstanceCore"),
      ],
    });

    emrEc2Role.addToPolicy(
      new iam.PolicyStatement({
        actions: [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:BatchGetPartition",
          "glue:CreateDatabase",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:CreatePartition",
          "glue:BatchCreatePartition",
          "glue:UpdatePartition",
          "glue:DeletePartition",
        ],
        resources: ["*"],
      }),
    );

    const emrInstanceProfile = new iam.CfnInstanceProfile(this, "EmrInstanceProfile", {
      roles: [emrEc2Role.roleName],
      instanceProfileName: `${namePrefix}-emr-ec2`,
    });

    emrLogsBucket.grantReadWrite(emrEc2Role);
    sharedKey.grantEncryptDecrypt(emrEc2Role);

    const trinoCluster = new emr.CfnCluster(this, "TrinoCluster", {
      name: `${namePrefix}-trino`,
      releaseLabel: emrReleaseLabel,
      applications: [{ name: "Hadoop" }, { name: "Hive" }, { name: "Trino" }],
      instances: {
        ec2SubnetId: vpc.privateSubnets[0].subnetId,
        keepJobFlowAliveWhenNoSteps: true,
        emrManagedMasterSecurityGroup: emrMasterSecurityGroup.securityGroupId,
        emrManagedSlaveSecurityGroup: emrCoreSecurityGroup.securityGroupId,
        serviceAccessSecurityGroup: emrServiceAccessSecurityGroup.securityGroupId,
        masterInstanceGroup: {
          instanceCount: 1,
          instanceType: trinoInstanceType,
          name: "Primary",
        },
        coreInstanceGroup: {
          instanceCount: trinoCoreInstanceCount,
          instanceType: trinoInstanceType,
          name: "Core",
        },
      },
      configurations: [
        {
          classification: "hive-site",
          configurationProperties: {
            "hive.metastore.client.factory.class":
              "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
          },
        },
        {
          classification: "trino-connector-hive",
          configurationProperties: {
            "hive.metastore": "glue",
          },
        },
      ],
      ebsRootVolumeSize: 64,
      visibleToAllUsers: true,
      logUri: `s3://${emrLogsBucket.bucketName}/emr/`,
      serviceRole: emrServiceRole.roleName,
      jobFlowRole: emrInstanceProfile.ref,
    });

    trinoCluster.addDependency(emrInstanceProfile);

    const trinoHost = trinoCluster.attrMasterPublicDns;
    const trinoJdbcQuery = "protocol=https&verify=false";

    const ecsCluster = new ecs.Cluster(this, "AppsCluster", { vpc });

    const marquezApiTaskDefinition = new ecs.FargateTaskDefinition(this, "MarquezApiTask", {
      cpu: 512,
      memoryLimitMiB: 1024,
      runtimePlatform: {
        operatingSystemFamily: ecs.OperatingSystemFamily.LINUX,
        cpuArchitecture: ecs.CpuArchitecture.X86_64,
      },
    });

    marquezApiTaskDefinition.addContainer("MarquezApiContainer", {
      image: ecs.ContainerImage.fromRegistry(marquezApiImage),
      logging: ecs.LogDrivers.awsLogs({ streamPrefix: "marquez-api" }),
      environment: {
        MARQUEZ_PORT: "5000",
        MARQUEZ_ADMIN_PORT: "5001",
        POSTGRES_HOST: metadataDb.instanceEndpoint.hostname,
        POSTGRES_PORT: metadataDb.instanceEndpoint.port.toString(),
        POSTGRES_DB: "platform",
        SEARCH_ENABLED: "false",
      },
      secrets: {
        POSTGRES_USER: ecs.Secret.fromSecretsManager(metadataDbSecret, "username"),
        POSTGRES_PASSWORD: ecs.Secret.fromSecretsManager(metadataDbSecret, "password"),
      },
      portMappings: [{ containerPort: 5000 }],
    });

    const marquezApiService = new ecs.FargateService(this, "MarquezApiService", {
      cluster: ecsCluster,
      taskDefinition: marquezApiTaskDefinition,
      desiredCount: marquezApiDesiredCount,
      assignPublicIp: false,
      securityGroups: [appSecurityGroup],
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
      cloudMapOptions: {
        cloudMapNamespace: serviceNamespace,
        name: "marquez-api",
        dnsRecordType: servicediscovery.DnsRecordType.A,
      },
      enableExecuteCommand: true,
      circuitBreaker: { rollback: true },
    });

    const marquezApiAutoscaling = marquezApiService.autoScaleTaskCount({
      minCapacity: marquezApiMinCapacity,
      maxCapacity: marquezApiMaxCapacity,
    });
    marquezApiAutoscaling.scaleOnCpuUtilization("MarquezApiCpuScaling", {
      targetUtilizationPercent: 65,
      scaleInCooldown: cdk.Duration.minutes(2),
      scaleOutCooldown: cdk.Duration.minutes(1),
    });

    const marquezApiDnsName = `marquez-api.${serviceNamespace.namespaceName}`;

    const marquezWebService = new ecsPatterns.ApplicationLoadBalancedFargateService(
      this,
      "MarquezWebService",
      {
        cluster: ecsCluster,
        taskSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
        desiredCount: 1,
        publicLoadBalancer: publicUis,
        listenerPort: 80,
        openListener: publicUis,
        assignPublicIp: false,
        securityGroups: [appSecurityGroup],
        taskImageOptions: {
          containerName: "marquez-web",
          image: ecs.ContainerImage.fromRegistry(marquezWebImage),
          containerPort: 3000,
          logDriver: ecs.LogDrivers.awsLogs({ streamPrefix: "marquez-web" }),
          environment: {
            MARQUEZ_HOST: marquezApiDnsName,
            MARQUEZ_PORT: "5000",
            WEB_PORT: "3000",
          },
        },
      },
    );

    if (!publicUis) {
      marquezWebService.listener.connections.allowDefaultPortFrom(
        ec2.Peer.ipv4(vpc.vpcCidrBlock),
        "Allow access to internal Marquez UI from within the VPC.",
      );
    }

    marquezWebService.targetGroup.configureHealthCheck({
      path: "/",
      healthyHttpCodes: "200-399",
      interval: cdk.Duration.seconds(30),
    });

    const supersetImagePath = path.join(repoRootPath, "infra", "superset");
    const trinoSqlalchemyUri = cdk.Fn.join("", [
      "trino://superset@",
      trinoHost,
      ":8889/tf2/default?",
      trinoJdbcQuery,
    ]);

    const supersetStartCommand =
      "superset db upgrade && " +
      "(superset fab create-admin " +
      '--username "$SUPERSET_ADMIN_USERNAME" ' +
      '--firstname "$SUPERSET_ADMIN_FIRSTNAME" ' +
      '--lastname "$SUPERSET_ADMIN_LASTNAME" ' +
      '--email "$SUPERSET_ADMIN_EMAIL" ' +
      '--password "$SUPERSET_ADMIN_PASSWORD" || true) && ' +
      "superset init && " +
      "superset run -h 0.0.0.0 -p 8088";

    const supersetService = new ecsPatterns.ApplicationLoadBalancedFargateService(
      this,
      "SupersetService",
      {
        cluster: ecsCluster,
        taskSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
        desiredCount: 1,
        publicLoadBalancer: publicUis,
        listenerPort: 80,
        openListener: publicUis,
        assignPublicIp: false,
        securityGroups: [appSecurityGroup],
        taskImageOptions: {
          containerName: "superset",
          image: ecs.ContainerImage.fromAsset(supersetImagePath),
          containerPort: 8088,
          logDriver: ecs.LogDrivers.awsLogs({ streamPrefix: "superset" }),
          command: ["/bin/sh", "-c", supersetStartCommand],
          environment: {
            SUPERSET_CONFIG_PATH: "/app/pythonpath/superset_config.py",
            SUPERSET_DB_HOST: metadataDb.instanceEndpoint.hostname,
            SUPERSET_DB_PORT: metadataDb.instanceEndpoint.port.toString(),
            SUPERSET_DB_NAME: "platform",
            SUPERSET_TRINO_SQLALCHEMY_URI: trinoSqlalchemyUri,
          },
          secrets: {
            SUPERSET_DB_USER: ecs.Secret.fromSecretsManager(metadataDbSecret, "username"),
            SUPERSET_DB_PASSWORD: ecs.Secret.fromSecretsManager(metadataDbSecret, "password"),
            SUPERSET_SECRET_KEY: ecs.Secret.fromSecretsManager(supersetSecretKey),
            SUPERSET_ADMIN_USERNAME: ecs.Secret.fromSecretsManager(supersetAdminSecret, "username"),
            SUPERSET_ADMIN_PASSWORD: ecs.Secret.fromSecretsManager(supersetAdminSecret, "password"),
            SUPERSET_ADMIN_FIRSTNAME: ecs.Secret.fromSecretsManager(
              supersetAdminSecret,
              "firstname",
            ),
            SUPERSET_ADMIN_LASTNAME: ecs.Secret.fromSecretsManager(supersetAdminSecret, "lastname"),
            SUPERSET_ADMIN_EMAIL: ecs.Secret.fromSecretsManager(supersetAdminSecret, "email"),
          },
        },
      },
    );

    if (!publicUis) {
      supersetService.listener.connections.allowDefaultPortFrom(
        ec2.Peer.ipv4(vpc.vpcCidrBlock),
        "Allow access to internal Superset UI from within the VPC.",
      );
    }

    supersetService.targetGroup.configureHealthCheck({
      path: "/health",
      healthyHttpCodes: "200",
      interval: cdk.Duration.seconds(30),
    });

    const supersetAutoscaling = supersetService.service.autoScaleTaskCount({
      minCapacity: 1,
      maxCapacity: 4,
    });
    supersetAutoscaling.scaleOnCpuUtilization("SupersetCpuScaling", {
      targetUtilizationPercent: 65,
      scaleInCooldown: cdk.Duration.minutes(5),
      scaleOutCooldown: cdk.Duration.minutes(2),
    });

    let mwaaEnvironment: mwaa.CfnEnvironment | undefined;
    let trinoConnectionSecret: secretsmanager.Secret | undefined;

    if (enableMwaa && mwaaBucket && mwaaSecurityGroup) {
      const airflowConnectionPrefix = `${namePrefix}/airflow/connections`;
      trinoConnectionSecret = new secretsmanager.Secret(this, "AirflowTrinoConnectionSecret", {
        secretName: `${airflowConnectionPrefix}/trino_default`,
        encryptionKey: sharedKey,
        secretStringValue: cdk.SecretValue.unsafePlainText(
          cdk.Fn.join("", ["trino://airflow@", trinoHost, ":8889/tf2/default?", trinoJdbcQuery]),
        ),
      });

      const mwaaExecutionRole = new iam.Role(this, "MwaaExecutionRole", {
        assumedBy: new iam.CompositePrincipal(
          new iam.ServicePrincipal("airflow.amazonaws.com"),
          new iam.ServicePrincipal("airflow-env.amazonaws.com"),
        ),
        description: "Execution role for MWAA DAGs, secrets backend, and OpenLineage transport.",
      });

      mwaaBucket.grantReadWrite(mwaaExecutionRole);
      trinoConnectionSecret.grantRead(mwaaExecutionRole);
      sharedKey.grantEncryptDecrypt(mwaaExecutionRole);

      mwaaExecutionRole.addToPolicy(
        new iam.PolicyStatement({
          actions: ["airflow:PublishMetrics"],
          resources: [
            `arn:aws:airflow:${cdk.Stack.of(this).region}:${cdk.Stack.of(this).account}:environment/${namePrefix}-mwaa`,
          ],
        }),
      );

      mwaaExecutionRole.addToPolicy(
        new iam.PolicyStatement({
          actions: [
            "logs:CreateLogStream",
            "logs:CreateLogGroup",
            "logs:PutLogEvents",
            "logs:GetLogEvents",
            "logs:GetLogRecord",
            "logs:GetLogGroupFields",
            "logs:GetQueryResults",
          ],
          resources: [
            `arn:aws:logs:${cdk.Stack.of(this).region}:${cdk.Stack.of(this).account}:log-group:airflow-${namePrefix}-*`,
          ],
        }),
      );

      mwaaExecutionRole.addToPolicy(
        new iam.PolicyStatement({
          actions: [
            "sqs:ChangeMessageVisibility",
            "sqs:DeleteMessage",
            "sqs:GetQueueAttributes",
            "sqs:GetQueueUrl",
            "sqs:ReceiveMessage",
            "sqs:SendMessage",
          ],
          resources: [
            `arn:aws:sqs:${cdk.Stack.of(this).region}:${cdk.Stack.of(this).account}:airflow-celery-*`,
          ],
        }),
      );

      mwaaExecutionRole.addToPolicy(
        new iam.PolicyStatement({
          actions: [
            "secretsmanager:GetSecretValue",
            "secretsmanager:DescribeSecret",
            "secretsmanager:ListSecrets",
          ],
          resources: [trinoConnectionSecret.secretArn],
        }),
      );

      new s3deploy.BucketDeployment(this, "MwaaRequirementsDeployment", {
        destinationBucket: mwaaBucket,
        destinationKeyPrefix: "bootstrap",
        sources: [s3deploy.Source.asset(path.join(repoRootPath, "infra", "aws", "assets", "mwaa"))],
        prune: false,
        retainOnDelete: false,
      });

      new s3deploy.BucketDeployment(this, "MwaaDagsDeployment", {
        destinationBucket: mwaaBucket,
        destinationKeyPrefix: "dags",
        sources: [s3deploy.Source.asset(path.join(repoRootPath, "infra", "airflow", "dags"))],
        prune: false,
        retainOnDelete: false,
      });

      const openlineageTransport = cdk.Stack.of(this).toJsonString({
        type: "http",
        url: `http://${marquezApiDnsName}:5000`,
        endpoint: "api/v1/lineage",
      });

      mwaaEnvironment = new mwaa.CfnEnvironment(this, "MwaaEnvironment", {
        name: `${namePrefix}-mwaa`,
        airflowVersion: mwaaAirflowVersion,
        environmentClass: mwaaEnvironmentClass,
        sourceBucketArn: mwaaBucket.bucketArn,
        dagS3Path: "dags",
        requirementsS3Path: "bootstrap/requirements.txt",
        executionRoleArn: mwaaExecutionRole.roleArn,
        networkConfiguration: {
          securityGroupIds: [mwaaSecurityGroup.securityGroupId],
          subnetIds: [vpc.privateSubnets[0].subnetId, vpc.privateSubnets[1].subnetId],
        },
        webserverAccessMode: "PRIVATE_ONLY",
        minWorkers: mwaaMinWorkers,
        maxWorkers: mwaaMaxWorkers,
        schedulers: mwaaSchedulers,
        loggingConfiguration: {
          dagProcessingLogs: {
            enabled: true,
            logLevel: "INFO",
          },
          schedulerLogs: {
            enabled: true,
            logLevel: "INFO",
          },
          taskLogs: {
            enabled: true,
            logLevel: "INFO",
          },
          webserverLogs: {
            enabled: true,
            logLevel: "INFO",
          },
          workerLogs: {
            enabled: true,
            logLevel: "INFO",
          },
        },
        airflowConfigurationOptions: {
          "core.load_examples": "False",
          "webserver.expose_config": "False",
          "openlineage.disabled": "False",
          "openlineage.namespace": `${namePrefix}-airflow`,
          "openlineage.transport": openlineageTransport,
          "secrets.backend":
            "airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend",
          "secrets.backend_kwargs": cdk.Stack.of(this).toJsonString({
            connections_prefix: airflowConnectionPrefix,
          }),
        },
      });
    }

    new cdk.CfnOutput(this, "VpcId", {
      value: vpc.vpcId,
      description: "Shared VPC for all analytics platform services.",
    });

    new cdk.CfnOutput(this, "MetadataDbEndpoint", {
      value: metadataDb.instanceEndpoint.hostname,
      description: "PostgreSQL endpoint for Superset and Marquez metadata.",
    });

    new cdk.CfnOutput(this, "TrinoCoordinatorDns", {
      value: trinoHost,
      description: "EMR Trino coordinator host (private DNS in VPC).",
    });

    new cdk.CfnOutput(this, "MarquezApiInternalUrl", {
      value: `http://${marquezApiDnsName}:5000/api/v1/lineage`,
      description: "Internal OpenLineage ingestion endpoint for MWAA and in-VPC producers.",
    });

    new cdk.CfnOutput(this, "MarquezWebUrl", {
      value: cdk.Fn.join("", ["http://", marquezWebService.loadBalancer.loadBalancerDnsName]),
      description: "Marquez web UI endpoint.",
    });

    new cdk.CfnOutput(this, "SupersetUrl", {
      value: cdk.Fn.join("", ["http://", supersetService.loadBalancer.loadBalancerDnsName]),
      description: "Superset web UI endpoint.",
    });

    if (mwaaEnvironment && trinoConnectionSecret) {
      new cdk.CfnOutput(this, "MwaaEnvironmentArn", {
        value: mwaaEnvironment.attrArn,
        description: "Amazon MWAA environment ARN.",
      });

      new cdk.CfnOutput(this, "MwaaWebserverUrl", {
        value: mwaaEnvironment.attrWebserverUrl,
        description: "Airflow webserver URL generated by MWAA.",
      });

      new cdk.CfnOutput(this, "AirflowTrinoConnectionSecretArn", {
        value: trinoConnectionSecret.secretArn,
        description: "Secrets Manager key used by MWAA's secrets backend for trino_default.",
      });
    }
  }
}

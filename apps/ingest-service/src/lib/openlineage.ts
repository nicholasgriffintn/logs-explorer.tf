import type { IngestEnv, IngestMode, IngestResult } from "./types";

const RUN_EVENT_SCHEMA_URL =
  "https://openlineage.io/spec/2-0-2/OpenLineage.json#/definitions/RunEvent";
const CUSTOM_FACET_SCHEMA_URL = "https://logs-explorer.dev/spec/facets/ingest-run/1-0-0";
const DEFAULT_PRODUCER_URL = "https://github.com/OpenLineage/OpenLineage/tree/main/client";

interface LineageConfig {
  enabled: boolean;
  endpointUrl: string;
  namespace: string;
  jobName: string;
  datasetNamespace: string;
  producerUrl: string;
}

interface LineageRunMeta {
  mode: IngestMode;
  dryRun: boolean;
  trigger: string;
}

interface OpenLineageEvent {
  eventType: "START" | "COMPLETE" | "FAIL";
  eventTime: string;
  producer: string;
  schemaURL: string;
  run: {
    runId: string;
    facets: Record<string, unknown>;
  };
  job: {
    namespace: string;
    name: string;
    facets: Record<string, unknown>;
  };
  inputs?: Array<Record<string, unknown>>;
  outputs?: Array<Record<string, unknown>>;
}

function parseBool(value: string | undefined, fallback: boolean): boolean {
  if (value === undefined) {
    return fallback;
  }
  const lowered = value.trim().toLowerCase();
  if (["1", "true", "yes", "y", "on"].includes(lowered)) {
    return true;
  }
  if (["0", "false", "no", "n", "off"].includes(lowered)) {
    return false;
  }
  return fallback;
}

function trimSlashes(value: string): string {
  return value.replace(/^\/+|\/+$/g, "");
}

function makeConfig(env: IngestEnv): LineageConfig {
  const baseUrl = (env.OPENLINEAGE_URL ?? "").trim().replace(/\/+$/g, "");
  const enabledByUrl = baseUrl.length > 0;
  const enabled = parseBool(env.OPENLINEAGE_ENABLED, enabledByUrl);
  const endpoint = trimSlashes(env.OPENLINEAGE_ENDPOINT ?? "api/v1/lineage");

  if (!enabled || !baseUrl) {
    return {
      enabled: false,
      endpointUrl: "",
      namespace: "",
      jobName: "",
      datasetNamespace: "",
      producerUrl: "",
    };
  }

  return {
    enabled: true,
    endpointUrl: `${baseUrl}/${endpoint}`,
    namespace: (env.OPENLINEAGE_NAMESPACE ?? "tf2-ingest").trim() || "tf2-ingest",
    jobName: (env.OPENLINEAGE_JOB_NAME ?? "logs_tf_ingest").trim() || "logs_tf_ingest",
    datasetNamespace: (env.OPENLINEAGE_DATASET_NAMESPACE ?? "tf2").trim() || "tf2",
    producerUrl:
      (env.OPENLINEAGE_PRODUCER_URL ?? DEFAULT_PRODUCER_URL).trim() || DEFAULT_PRODUCER_URL,
  };
}

function runId(): string {
  if ("randomUUID" in crypto) {
    return crypto.randomUUID();
  }

  return `${Date.now()}-${Math.floor(Math.random() * 1_000_000_000)}`;
}

function customFacet(
  config: LineageConfig,
  payload: Record<string, unknown>,
): Record<string, unknown> {
  return {
    _producer: config.producerUrl,
    _schemaURL: CUSTOM_FACET_SCHEMA_URL,
    ...payload,
  };
}

function inputDatasets(): Array<Record<string, unknown>> {
  return [
    {
      namespace: "https://logs.tf",
      name: "api/v1/log",
      facets: {},
    },
  ];
}

function outputDatasets(config: LineageConfig): Array<Record<string, unknown>> {
  return [
    {
      namespace: config.datasetNamespace,
      name: "default.logs",
      facets: {},
    },
    {
      namespace: config.datasetNamespace,
      name: "default.messages",
      facets: {},
    },
    {
      namespace: config.datasetNamespace,
      name: "default.summaries",
      facets: {},
    },
  ];
}

async function postEvent(config: LineageConfig, payload: OpenLineageEvent): Promise<void> {
  const response = await fetch(config.endpointUrl, {
    method: "POST",
    headers: {
      "content-type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    const responseText = await response.text();
    throw new Error(
      `OpenLineage endpoint returned ${response.status} ${response.statusText}: ${responseText.slice(0, 300)}`,
    );
  }
}

async function postIfEnabled(config: LineageConfig, payload: OpenLineageEvent): Promise<void> {
  if (!config.enabled) {
    return;
  }

  await postEvent(config, payload);
}

function startEvent(
  config: LineageConfig,
  runIdentifier: string,
  eventTime: string,
  meta: LineageRunMeta,
): OpenLineageEvent {
  return {
    eventType: "START",
    eventTime,
    producer: config.producerUrl,
    schemaURL: RUN_EVENT_SCHEMA_URL,
    run: {
      runId: runIdentifier,
      facets: {
        ingestRun: customFacet(config, {
          mode: meta.mode,
          dryRun: meta.dryRun,
          trigger: meta.trigger,
        }),
      },
    },
    job: {
      namespace: config.namespace,
      name: config.jobName,
      facets: {},
    },
    inputs: inputDatasets(),
    outputs: outputDatasets(config),
  };
}

function completeEvent(
  config: LineageConfig,
  runIdentifier: string,
  eventTime: string,
  meta: LineageRunMeta,
  result: IngestResult,
): OpenLineageEvent {
  return {
    eventType: "COMPLETE",
    eventTime,
    producer: config.producerUrl,
    schemaURL: RUN_EVENT_SCHEMA_URL,
    run: {
      runId: runIdentifier,
      facets: {
        ingestRun: customFacet(config, {
          mode: meta.mode,
          dryRun: meta.dryRun,
          trigger: meta.trigger,
          fetchedNewLogs: result.fetchedNewLogs,
          retriedLogs: result.retriedLogs,
          emittedCoreLogs: result.emittedCoreLogs,
          emittedChatMessages: result.emittedChatMessages,
          emittedPlayerSummaries: result.emittedPlayerSummaries,
          failedLogs: result.failedLogs,
          lastIngestedLogId: result.lastIngestedLogId,
        }),
      },
    },
    job: {
      namespace: config.namespace,
      name: config.jobName,
      facets: {},
    },
    inputs: inputDatasets(),
    outputs: outputDatasets(config),
  };
}

function failEvent(
  config: LineageConfig,
  runIdentifier: string,
  eventTime: string,
  meta: LineageRunMeta,
  error: unknown,
): OpenLineageEvent {
  const message = error instanceof Error ? error.message : String(error);
  const stackTrace = error instanceof Error ? (error.stack ?? "") : "";

  return {
    eventType: "FAIL",
    eventTime,
    producer: config.producerUrl,
    schemaURL: RUN_EVENT_SCHEMA_URL,
    run: {
      runId: runIdentifier,
      facets: {
        ingestRun: customFacet(config, {
          mode: meta.mode,
          dryRun: meta.dryRun,
          trigger: meta.trigger,
        }),
        errorMessage: {
          _producer: config.producerUrl,
          _schemaURL: "https://openlineage.io/spec/facets/1-0-0/ErrorMessageRunFacet.json",
          message,
          programmingLanguage: "typescript",
          stackTrace,
        },
      },
    },
    job: {
      namespace: config.namespace,
      name: config.jobName,
      facets: {},
    },
    inputs: inputDatasets(),
    outputs: outputDatasets(config),
  };
}

export interface IngestLineageEmitter {
  runId: string;
  emitStart(eventTime: string, meta: LineageRunMeta): Promise<void>;
  emitComplete(eventTime: string, meta: LineageRunMeta, result: IngestResult): Promise<void>;
  emitFail(eventTime: string, meta: LineageRunMeta, error: unknown): Promise<void>;
}

export function createIngestLineageEmitter(env: IngestEnv): IngestLineageEmitter {
  const config = makeConfig(env);
  const currentRunId = runId();

  return {
    runId: currentRunId,
    emitStart(eventTime, meta) {
      return postIfEnabled(config, startEvent(config, currentRunId, eventTime, meta));
    },
    emitComplete(eventTime, meta, result) {
      return postIfEnabled(config, completeEvent(config, currentRunId, eventTime, meta, result));
    },
    emitFail(eventTime, meta, error) {
      return postIfEnabled(config, failEvent(config, currentRunId, eventTime, meta, error));
    },
  };
}

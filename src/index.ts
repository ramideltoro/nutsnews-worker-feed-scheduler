import { pathToFileURL } from "node:url";

import {
  createJsonRuntimeTelemetrySink,
  createPrometheusRuntimeTelemetrySink,
  createRuntimeShutdownController,
  getRuntimePackageMetadata,
  SYSTEM_RUNTIME_CLOCK,
  type RuntimeServiceIdentity
} from "@ramideltoro/nutsnews-worker-runtime";
import { getContractPackageMetadata } from "@ramideltoro/nutsnews-worker-contracts";

import {
  loadSchedulerConfig,
  type SchedulerConfig
} from "./config.js";
import type { SchedulerDependencies } from "./dependencies.js";
import { createSchedulerHttpServer } from "./http.js";
import { createSchedulerLoop } from "./loop.js";
import {
  createSchedulerDependencies
} from "./production-dependencies.js";
import {
  createSchedulerFailClosedReconciler
} from "./reconciliation.js";
import {
  SCHEDULER_HEALTH_CHECK_NAMES,
  createSchedulerService
} from "./service.js";
import {
  bestEffortSchedulerMetricsSink,
  bestEffortTelemetryFlusher
} from "./telemetry-safety.js";
import { createLocalSchedulerDependencies } from "./test-doubles.js";

export {
  SCHEDULER_CONFIG_SCHEMA,
  SCHEDULER_PRODUCTION_MIN_LEASE_MS,
  SCHEDULER_SERVICE_NAME,
  SCHEDULER_SERVICE_VERSION,
  loadSchedulerConfig,
  type SchedulerConfig
} from "./config.js";
export type {
  SchedulerDependencies,
  SchedulerFeedSource
} from "./dependencies.js";
export {
  createSchedulerHttpServer
} from "./http.js";
export {
  createSchedulerLoop,
  type SchedulerLoop,
  type SchedulerLoopOptions
} from "./loop.js";
export {
  BackendApiFeedSource,
  PostgresScheduleLeaseStore,
  SchedulerDependencyError,
  createProductionSchedulerDependencies,
  createSchedulerDependencies,
  type BackendApiFeedSourceOptions,
  type ProductionSchedulerEnvironment
} from "./production-dependencies.js";
export {
  SchedulerRabbitMqPublisherTransport,
  type SchedulerRabbitMqPublisherOptions
} from "./rabbitmq-publisher.js";
export {
  SchedulerPublishError,
  schedulerPublishDisposition,
  type SchedulerPublishDisposition
} from "./publish-error.js";
export {
  SCHEDULER_RECONCILIATION_CONFIRMATION,
  SCHEDULER_RECONCILIATION_PATH,
  createSchedulerFailClosedReconciler,
  type SchedulerReconciliationReport,
  type SchedulerReconciliationRequest,
  type SchedulerReconciler
} from "./reconciliation.js";
export {
  SCHEDULER_CYCLE_DURATION_BUCKETS_SECONDS,
  SCHEDULER_HEALTH_CHECK_NAMES,
  createSchedulerService,
  type SchedulerRunOnceResult,
  type SchedulerService
} from "./service.js";
export {
  LocalBrokerTransport,
  ManualSchedulerClock,
  createLocalFeedSource,
  createLocalDueFeeds,
  createLocalSchedulerDependencies
} from "./test-doubles.js";
export {
  SCHEDULER_FIXTURE_FEEDS,
  SCHEDULER_FIXTURE_NOW
} from "./fixtures.js";
export {
  InMemoryScheduleLeaseStore,
  SCHEDULE_LEASE_MAX_MS,
  ScheduleLeaseOwnershipError,
  assertScheduleLeaseDuration,
  type ScheduleLeaseRecord,
  type ScheduleLeaseStore
} from "./lease-store.js";
export {
  evaluateFeedSchedule,
  idempotencyKeyFor,
  scheduleWindowFor,
  selectDueFeeds,
  type SchedulerFeedDefinition,
  type FeedScheduleDecision
} from "./scheduling.js";
export {
  SequenceSchedulerIdFactory,
  createCryptoSchedulerIdFactory,
  type SchedulerIdFactory
} from "./ids.js";

export interface SchedulerApplication {
  readonly config: SchedulerConfig;
  start(): Promise<void>;
  stop(): Promise<void>;
  url(path?: string): string;
}

export function createSchedulerApplication(
  config = loadSchedulerConfig(),
  dependencyOverride?: SchedulerDependencies
): SchedulerApplication {
  const dependencies = dependencyOverride ?? createSchedulerDependencies(config);
  const identity: RuntimeServiceIdentity = {
    service: config.serviceName,
    version: config.serviceVersion,
    environment: config.environment,
    host: config.host,
    revision: config.buildRevision,
    deployment: config.shadowMode ? "shadow" : "production",
    adapter: runtimeAdapterMode(dependencies)
  };
  const logSink = bestEffortTelemetryFlusher(config.telemetryLogs === "stdout"
    ? createJsonRuntimeTelemetrySink({
        identity,
        writer: (line) => {
          console.log(line);
        }
      })
    : undefined);
  const metricsOptions = {
    identity,
    cardinality: {
      dependencies: [
        "scheduler-shell",
        "scheduler",
        "lease-store",
        "broker"
      ],
      healthChecks: Object.values(SCHEDULER_HEALTH_CHECK_NAMES).flat()
    },
    expectedActive: !config.shadowMode
  } as const;
  const metrics = bestEffortSchedulerMetricsSink(config.metricsEnabled
    ? createPrometheusRuntimeTelemetrySink(metricsOptions)
    : undefined);
  const telemetry = logSink;
  const reconciliationToken = reconciliationTokenFromEnv();
  const service = createSchedulerService({
    config,
    dependencies,
    ...(telemetry === undefined ? {} : {
      telemetry
    }),
    ...(metrics === undefined ? {} : {
      metrics
    })
  });
  const httpServer = createSchedulerHttpServer({
    config,
    service,
    reconciler: createSchedulerFailClosedReconciler(SYSTEM_RUNTIME_CLOCK),
    ...(reconciliationToken === undefined ? {} : {
      reconciliationToken
    }),
    ...(metrics === undefined ? {} : {
      metrics
    })
  });
  const schedulerLoop = createSchedulerLoop({
    service,
    cadenceMs: config.cadenceMs,
    onError: (error) => {
      console.error(JSON.stringify({
        event: "scheduler.run.failed",
        errorClass: error instanceof Error && error.name.length > 0
          ? error.name
          : "UnknownError",
        safeMetadataOnly: true
      }));
    }
  });
  const shutdown = createRuntimeShutdownController({
    callbacks: [
      async () => {
        await schedulerLoop.stop();
      },
      async () => {
        await service.stop();
      },
      async () => {
        await httpServer.close();
      }
    ],
    signalSource: process,
    timeoutMs: config.shutdownTimeoutMs,
    ...(telemetry === undefined ? {} : {
      telemetry
    }),
    ...(logSink === undefined ? {} : {
      telemetryFlusher: logSink
    })
  });

  return {
    config,
    async start(): Promise<void> {
      assertPackageCompatibility();
      await httpServer.listen();
      shutdown.start();

      try {
        await service.start();
        schedulerLoop.start();
      } catch (error: unknown) {
        shutdown.stop();
        await schedulerLoop.stop().catch(() => undefined);
        await service.stop().catch(() => undefined);
        await httpServer.close().catch(() => undefined);
        throw error;
      }
    },
    async stop(): Promise<void> {
      await shutdown.trigger("manual");
    },
    url: (path = "/") => httpServer.url(path)
  };
}

export function createSchedulerApplicationDependencies(): SchedulerDependencies {
  return {
    ...createLocalSchedulerDependencies(),
    clockKind: "system",
    clock: SYSTEM_RUNTIME_CLOCK
  };
}

function runtimeAdapterMode(dependencies: SchedulerDependencies): "in_memory" | "mixed" | "production" {
  const unique = new Set([
    dependencies.clockKind === "system" ? "production" : "in_memory",
    dependencies.feedSource.adapterKind === "backend-api" ? "production" : "in_memory",
    dependencies.leaseStore.adapterKind === "postgres" ? "production" : "in_memory",
    dependencies.brokerKind === "rabbitmq" ? "production" : "in_memory"
  ]);

  if (unique.size !== 1) {
    return "mixed";
  }

  return unique.values().next().value === "production" ? "production" : "in_memory";
}

function reconciliationTokenFromEnv(): string | undefined {
  const serviceToken = process.env.NUTSNEWS_SCHEDULER_RECONCILIATION_TOKEN?.trim();
  const globalToken = process.env.NUTSNEWS_WORKER_UPLIFT_RECONCILIATION_TOKEN?.trim();
  const token = serviceToken !== undefined && serviceToken.length > 0 ? serviceToken : globalToken;

  return token === undefined || token.length === 0 ? undefined : token;
}

function assertPackageCompatibility(): void {
  const contracts = getContractPackageMetadata();
  const runtime = getRuntimePackageMetadata();
  const contractsVersion: string = contracts.packageVersion;
  const runtimeVersion: string = runtime.packageVersion;
  const runtimeContractsVersion: string = runtime.contractsPackageVersion;

  if (contractsVersion !== "1.0.0") {
    throw new Error(`Unsupported contracts package version ${contractsVersion}.`);
  }

  if (runtimeVersion !== "1.0.0") {
    throw new Error(`Unsupported runtime package version ${runtimeVersion}.`);
  }

  if (runtimeContractsVersion !== contractsVersion) {
    throw new Error(
      `Runtime contracts version ${runtimeContractsVersion} does not match installed contracts ${contractsVersion}.`
    );
  }
}

if (process.argv[1] !== undefined && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const application = createSchedulerApplication();

  application.start().catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : "failed to start scheduler");
    process.exitCode = 1;
  });
}

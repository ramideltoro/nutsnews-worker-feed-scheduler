import { pathToFileURL } from "node:url";

import {
  createJsonRuntimeTelemetrySink,
  createPrometheusRuntimeTelemetrySink,
  createRuntimeShutdownController,
  getRuntimePackageMetadata,
  type RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";
import { getContractPackageMetadata } from "@ramideltoro/nutsnews-worker-contracts";

import {
  loadSchedulerConfig,
  type SchedulerConfig
} from "./config.js";
import { createSchedulerHttpServer } from "./http.js";
import { createSchedulerService } from "./service.js";
import { createLocalSchedulerDependencies } from "./test-doubles.js";

export {
  SCHEDULER_CONFIG_SCHEMA,
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
}

export function createSchedulerApplication(config = loadSchedulerConfig()): SchedulerApplication {
  const identity = {
    service: config.serviceName,
    version: config.serviceVersion,
    environment: config.environment,
    host: config.host
  };
  const logSink = config.telemetryLogs === "stdout"
    ? createJsonRuntimeTelemetrySink({
        identity,
        writer: (line) => {
          console.log(line);
        }
      })
    : undefined;
  const metrics = config.metricsEnabled
    ? createPrometheusRuntimeTelemetrySink({
        identity
      })
    : undefined;
  const telemetry = combineTelemetrySinks(logSink, metrics);
  const dependencies = createLocalSchedulerDependencies();
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
    ...(metrics === undefined ? {} : {
      metrics
    })
  });
  const shutdown = createRuntimeShutdownController({
    callbacks: [
      async () => {
        await httpServer.close();
      },
      async () => {
        await service.stop();
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
      await service.start();
      await httpServer.listen();
      shutdown.start();
    },
    async stop(): Promise<void> {
      await shutdown.trigger("manual");
    }
  };
}

function combineTelemetrySinks(
  ...sinks: readonly (RuntimeTelemetrySink | undefined)[]
): RuntimeTelemetrySink | undefined {
  const configured = sinks.filter((sink): sink is RuntimeTelemetrySink => sink !== undefined);

  if (configured.length === 0) {
    return undefined;
  }

  return {
    emit: async (event) => {
      for (const sink of configured) {
        await sink.emit(event);
      }
    }
  };
}

function assertPackageCompatibility(): void {
  const contracts = getContractPackageMetadata();
  const runtime = getRuntimePackageMetadata();
  const contractsVersion: string = contracts.packageVersion;
  const runtimeVersion: string = runtime.packageVersion;

  if (contractsVersion !== "0.3.1") {
    throw new Error(`Unsupported contracts package version ${contractsVersion}.`);
  }

  if (runtimeVersion !== "0.4.0") {
    throw new Error(`Unsupported runtime package version ${runtimeVersion}.`);
  }
}

if (process.argv[1] !== undefined && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const application = createSchedulerApplication();

  application.start().catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : "failed to start scheduler");
    process.exitCode = 1;
  });
}

import { getWorkerRoute } from "@ramideltoro/nutsnews-worker-contracts";
import {
  createBrokerLifecycle,
  createRuntimeHealthProbeSet,
  createRuntimeInFlightDrainController,
  emitRuntimeTelemetry,
  runtimeNow,
  type BrokerLifecycle,
  type PrometheusRuntimeTelemetrySink,
  type RuntimeHealthCheck,
  type RuntimeHealthProbeSet,
  type RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";

import type { SchedulerConfig } from "./config.js";
import type { SchedulerDependencies } from "./dependencies.js";

export interface SchedulerServiceOptions {
  readonly config: SchedulerConfig;
  readonly dependencies: SchedulerDependencies;
  readonly telemetry?: RuntimeTelemetrySink;
  readonly metrics?: PrometheusRuntimeTelemetrySink;
}

export interface SchedulerRunOnceResult {
  readonly checkedAt: string;
  readonly dueFeedCount: number;
  readonly shadowMode: boolean;
}

export interface SchedulerService {
  readonly broker: BrokerLifecycle;
  readonly health: RuntimeHealthProbeSet;
  readonly isStarted: boolean;
  readonly isDraining: boolean;
  start(): Promise<void>;
  runOnce(): Promise<SchedulerRunOnceResult>;
  stop(): Promise<void>;
}

export function createSchedulerService(options: SchedulerServiceOptions): SchedulerService {
  const fetchRoute = getWorkerRoute("fetch");
  const broker = createBrokerLifecycle({
    transport: options.dependencies.brokerTransport,
    routes: [
      fetchRoute
    ],
    clock: options.dependencies.clock,
    ...(options.telemetry === undefined ? {} : {
      telemetry: options.telemetry
    })
  });
  const drain = createRuntimeInFlightDrainController({
    timeoutMs: options.config.shutdownTimeoutMs
  });
  let started = false;

  const service = {
    get broker(): BrokerLifecycle {
      return broker;
    },
    get health(): RuntimeHealthProbeSet {
      return createRuntimeHealthProbeSet({
        livenessChecks: [
          livenessCheck()
        ],
        startupChecks: [
          startupCheck(() => started)
        ],
        readinessChecks: [
          brokerReadinessCheck(broker),
          feedSourceReadinessCheck(options.dependencies),
          shadowModeCheck(options.config)
        ],
        clock: options.dependencies.clock,
        ...(options.telemetry === undefined ? {} : {
          telemetry: options.telemetry
        })
      });
    },
    get isStarted(): boolean {
      return started;
    },
    get isDraining(): boolean {
      return drain.isDraining;
    },
    async start(): Promise<void> {
      if (started) {
        return;
      }

      await broker.start();
      started = true;
      options.metrics?.setInFlight(fetchRoute.mainQueue.name, drain.inFlight);
      await emitRuntimeTelemetry(options.telemetry, {
        name: "runtime.dependency.observed",
        level: "info",
        at: runtimeNow(options.dependencies.clock),
        stage: "fetch",
        queue: fetchRoute.mainQueue.name,
        outcome: "success",
        attributes: {
          dependency: "scheduler-shell",
          mode: options.config.dependencyMode
        }
      });
    },
    async runOnce(): Promise<SchedulerRunOnceResult> {
      return drain.track(async () => {
        options.metrics?.setInFlight(fetchRoute.mainQueue.name, drain.inFlight);
        const checkedAt = runtimeNow(options.dependencies.clock);
        const dueFeedCount = await options.dependencies.feedSource.countDueFeeds(options.dependencies.clock.now());

        await emitRuntimeTelemetry(options.telemetry, {
          name: "runtime.dependency.observed",
          level: "info",
          at: checkedAt,
          stage: "fetch",
          queue: fetchRoute.mainQueue.name,
          outcome: "success",
          attributes: {
            dependency: "feed-source",
            dueFeedCount,
            shadowMode: options.config.shadowMode
          }
        });

        options.metrics?.recordDependencyLatency(fetchRoute.mainQueue.name, 0, "success");
        options.metrics?.setInFlight(fetchRoute.mainQueue.name, drain.inFlight);

        return {
          checkedAt,
          dueFeedCount,
          shadowMode: options.config.shadowMode
        };
      });
    },
    async stop(): Promise<void> {
      if (!started && broker.state === "closed") {
        return;
      }

      drain.stopAcceptingWork();
      options.metrics?.setShutdownDraining(true);
      await drain.waitForDrain(options.config.shutdownTimeoutMs);
      await broker.stop("shutdown");
      options.metrics?.setShutdownDraining(false);
      started = false;
    }
  } satisfies SchedulerService;

  return service;
}

function livenessCheck(): RuntimeHealthCheck {
  return {
    name: "process",
    critical: true,
    check: () => "ok"
  };
}

function startupCheck(isStarted: () => boolean): RuntimeHealthCheck {
  return {
    name: "service-started",
    critical: true,
    check: () => isStarted() ? "ok" : "unhealthy"
  };
}

function brokerReadinessCheck(broker: BrokerLifecycle): RuntimeHealthCheck {
  return {
    name: "broker-lifecycle",
    critical: true,
    check: () => broker.state === "ready"
      ? {
          status: "ok",
          details: {
            state: broker.state
          }
        }
      : {
          status: "unhealthy",
          details: {
            state: broker.state
          }
        }
  };
}

function feedSourceReadinessCheck(dependencies: SchedulerDependencies): RuntimeHealthCheck {
  return {
    name: "feed-source",
    critical: true,
    check: async () => {
      const probe = await dependencies.feedSource.probe();

      return {
        status: probe.status,
        details: {
          source: dependencies.feedSource.name,
          summary: probe.summary
        }
      };
    }
  };
}

function shadowModeCheck(config: SchedulerConfig): RuntimeHealthCheck {
  return {
    name: "shadow-mode",
    critical: true,
    check: () => config.shadowMode
      ? "ok"
      : {
          status: "unhealthy",
          details: {
            reason: "shadow-mode-disabled"
          }
        }
  };
}

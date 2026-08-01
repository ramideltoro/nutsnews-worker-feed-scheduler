import { performance } from "node:perf_hooks";

import {
  STAGE_PAYLOAD_SCHEMA_IDS,
  STAGE_PAYLOAD_SCHEMA_VERSION,
  WORKER_DELIVERY_BEHAVIOR,
  assertWorkerEnvelope,
  getStagePayloadSizeBytes,
  getWorkerRoute,
  validateStagePayload,
  type WorkerMessageEnvelope
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  createBrokerLifecycle,
  createRuntimeHealthProbeSet,
  createRuntimeInFlightDrainController,
  emitRuntimeTelemetry,
  runtimeNow,
  SYSTEM_RUNTIME_CLOCK,
  type BrokerLifecycle,
  type RuntimeClock,
  type RuntimeHealthCheck,
  type RuntimeHealthProbeSet,
  type RuntimeHealthProbe,
  type RuntimeHealthStatus,
  type RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";

import type { SchedulerConfig } from "./config.js";
import type { SchedulerDependencies } from "./dependencies.js";
import {
  createCryptoSchedulerIdFactory,
  type SchedulerIdFactory
} from "./ids.js";
import {
  selectDueFeeds,
  type DueFeedDecision,
  type FeedScheduleDecision
} from "./scheduling.js";
import {
  bestEffortSchedulerMetricsSink,
  combineBestEffortTelemetrySinks,
  type SchedulerMetricsSink
} from "./telemetry-safety.js";

export const SCHEDULER_CYCLE_DURATION_BUCKETS_SECONDS = [
  0.005,
  0.01,
  0.025,
  0.05,
  0.1,
  0.25,
  0.5,
  1,
  2.5,
  5,
  10,
  30,
  60,
  120,
  300
] as const;

type SchedulerCycleOutcome = "success" | "failure";
interface SchedulerCycleHistogram {
  readonly buckets: number[];
  count: number;
  sum: number;
}

export interface SchedulerServiceOptions {
  readonly config: SchedulerConfig;
  readonly dependencies: SchedulerDependencies;
  readonly idFactory?: SchedulerIdFactory;
  readonly telemetry?: RuntimeTelemetrySink;
  readonly metrics?: SchedulerMetricsSink;
}

export interface SchedulerRunOnceResult {
  readonly checkedAt: string;
  readonly dueFeedCount: number;
  readonly scheduledCount: number;
  readonly confirmedCount: number;
  readonly skippedCount: number;
  readonly failedCount: number;
  readonly shadowMode: boolean;
  readonly decisions: readonly FeedScheduleDecision[];
}

export interface SchedulerService {
  readonly broker: BrokerLifecycle;
  readonly health: RuntimeHealthProbeSet;
  readonly isStarted: boolean;
  readonly isDraining: boolean;
  readonly isSchedulingLoopActive: boolean;
  readonly lastSuccessAt: string | undefined;
  start(): Promise<void>;
  setSchedulingLoopActive?(active: boolean): void;
  /** Compatibility helper for isolated tests. Production scheduling is owned by createSchedulerLoop(). */
  startScheduling(): Promise<void>;
  runOnce(): Promise<SchedulerRunOnceResult>;
  collectOperationalMetrics(): string;
  stop(): Promise<void>;
}

export function createSchedulerService(options: SchedulerServiceOptions): SchedulerService {
  assertSchedulerDependencyBoundary(options.config, options.dependencies);
  const fetchRoute = getWorkerRoute("fetch");
  const idFactory = options.idFactory ?? createCryptoSchedulerIdFactory();
  const metrics = bestEffortSchedulerMetricsSink(options.metrics);
  const telemetry = combineBestEffortTelemetrySinks(options.telemetry, metrics);
  const broker = createBrokerLifecycle({
    transport: options.dependencies.brokerTransport,
    routes: [
      fetchRoute
    ],
    clock: options.dependencies.clock,
    ...(telemetry === undefined ? {} : {
      telemetry
    })
  });
  const drain = createRuntimeInFlightDrainController({
    timeoutMs: options.config.shutdownTimeoutMs
  });
  let started = false;
  let schedulingLoopActive = false;
  let lifecycleGeneration = 0;
  let lastSuccessAt: string | undefined;
  const cycleHistograms = new Map<SchedulerCycleOutcome, SchedulerCycleHistogram>();
  recordRuntimeProbeMetric(metrics, options.dependencies.clock, "liveness", "ok");
  recordRuntimeProbeMetric(metrics, options.dependencies.clock, "startup", "unhealthy");
  recordRuntimeProbeMetric(metrics, options.dependencies.clock, "readiness", "unhealthy");

  const service: SchedulerService = {
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
          brokerDependencyReadinessCheck(options.dependencies),
          feedSourceReadinessCheck(options.dependencies),
          leaseStoreReadinessCheck(options.dependencies),
          runtimeClockReadinessCheck(options.config, options.dependencies.clock),
          schedulingLoopReadinessCheck(
            options.config,
            options.dependencies,
            () => schedulingLoopActive,
            () => lastSuccessAt
          ),
          productionAdaptersReadinessCheck(options.config, options.dependencies)
        ],
        clock: options.dependencies.clock,
        ...(telemetry === undefined ? {} : {
          telemetry
        })
      });
    },
    get isStarted(): boolean {
      return started;
    },
    get isDraining(): boolean {
      return drain.isDraining;
    },
    get isSchedulingLoopActive(): boolean {
      return schedulingLoopActive;
    },
    get lastSuccessAt(): string | undefined {
      return lastSuccessAt;
    },
    async start(): Promise<void> {
      if (started) {
        return;
      }

      const startGeneration = lifecycleGeneration + 1;

      lifecycleGeneration = startGeneration;

      if (options.config.dependencyMode === "production") {
        const dependencyProbes = await Promise.all([
          options.dependencies.feedSource.probe(),
          options.dependencies.leaseStore.probe()
        ]);

        if (dependencyProbes.some((probe) => probe.status !== "ok")) {
          throw new Error("Production scheduler dependencies failed the startup probe.");
        }
      }

      await broker.start();

      if (startGeneration !== lifecycleGeneration) {
        await broker.stop("startup-cancelled").catch(() => undefined);
        const error = new Error("Scheduler startup was cancelled by a newer lifecycle transition.");

        error.name = "SchedulerStartCancelledError";
        throw error;
      }

      started = true;
      recordRuntimeProbeMetric(metrics, options.dependencies.clock, "startup", "ok");
      recordRuntimeProbeMetric(metrics, options.dependencies.clock, "readiness", "unhealthy");
      metrics?.setExpectedActive(!options.config.shadowMode);
      metrics?.setLastSuccessTimestamp(0);
      metrics?.setInFlight(fetchRoute.mainQueue.name, drain.inFlight);
      await emitRuntimeTelemetry(telemetry, {
        name: "runtime.dependency.observed",
        level: "info",
        at: runtimeNow(options.dependencies.clock),
        stage: "fetch",
        queue: fetchRoute.mainQueue.name,
        outcome: "success",
        attributes: {
          dependency: "scheduler-shell",
          mode: options.config.dependencyMode,
          adapter: aggregateAdapterMode(options.dependencies)
        }
      });
    },
    setSchedulingLoopActive(active: boolean): void {
      schedulingLoopActive = active;

      if (!active) {
        recordRuntimeProbeMetric(metrics, options.dependencies.clock, "readiness", "unhealthy");
      }
    },
    async startScheduling(): Promise<void> {
      if (!started) {
        throw new Error("Scheduler service must be started before its scheduling loop.");
      }

      if (schedulingLoopActive) {
        return;
      }

      schedulingLoopActive = true;
      await service.runOnce();
    },
    async runOnce(): Promise<SchedulerRunOnceResult> {
      const cycleStartedAt = performance.now();
      let cycleOutcome: SchedulerCycleOutcome = "failure";

      try {
        const result = await drain.track(async () => {
          metrics?.setInFlight(fetchRoute.mainQueue.name, drain.inFlight);
          const checkedAt = runtimeNow(options.dependencies.clock);
          const now = options.dependencies.clock.now();
          const feeds = await options.dependencies.feedSource.listActiveFeeds(now);
          const selection = selectDueFeeds(feeds, now, feeds.length);
          let scheduledCount = 0;
          let confirmedCount = 0;
          let failedCount = 0;
          let attemptedCount = 0;

          for (const decision of selection.skipped) {
            await emitRuntimeTelemetry(telemetry, {
              name: "runtime.dependency.observed",
              level: "info",
              at: checkedAt,
              stage: "fetch",
              queue: fetchRoute.mainQueue.name,
              outcome: "success",
              attributes: {
                event: "scheduler.feed.skipped",
                feedId: decision.feed.feedId,
                reason: decision.reason,
                nextEligibleAt: decision.nextEligibleAt
              }
            });
          }

          for (const decision of selection.due) {
            if (attemptedCount >= options.config.concurrency) {
              break;
            }

            const leaseResult = await acquireScheduleLease(
              options,
              telemetry,
              decision,
              now,
              checkedAt,
              fetchRoute.mainQueue.name
            );

            if (leaseResult === undefined) {
              attemptedCount += 1;
              failedCount += 1;
              continue;
            }

            if (leaseResult.status !== "acquired") {
              await emitRuntimeTelemetry(telemetry, {
                name: "runtime.dependency.observed",
                level: "info",
                at: checkedAt,
                stage: "fetch",
                queue: fetchRoute.mainQueue.name,
                outcome: "duplicate",
                attributes: {
                  event: "scheduler.feed.lease_skipped",
                  feedId: decision.feed.feedId,
                  reason: leaseResult.status,
                  windowStart: decision.window.start
                }
              });
              continue;
            }

            attemptedCount += 1;
            scheduledCount += 1;
            await emitRuntimeTelemetry(telemetry, {
              name: "runtime.dependency.observed",
              level: "info",
              at: checkedAt,
              stage: "fetch",
              queue: fetchRoute.mainQueue.name,
              outcome: "started",
              attributes: {
                event: "scheduler.feed.leased",
                dependency: "lease-store",
                feedId: decision.feed.feedId,
                windowStart: decision.window.start,
                attemptCount: leaseResult.record.attemptCount
              }
            });

            try {
              const command = createFetchPublishCommand(decision, idFactory, options.config, checkedAt);
              const receipt = await broker.publish(command);
              await options.dependencies.leaseStore.markConfirmed(leaseResult.record.token, options.dependencies.clock.now(), receipt.messageId);
              confirmedCount += 1;
              await emitRuntimeTelemetry(telemetry, {
                name: "runtime.message.accepted",
                level: "info",
                at: runtimeNow(options.dependencies.clock),
                stage: "fetch",
                queue: fetchRoute.mainQueue.name,
                messageId: command.envelope.messageId,
                idempotencyKey: command.envelope.idempotencyKey,
                correlationId: command.envelope.correlationId,
                traceparent: command.envelope.traceparent,
                outcome: "success",
                attributes: {
                  event: "scheduler.feed.confirmed",
                  dependency: "broker",
                  feedId: decision.feed.feedId,
                  windowStart: decision.window.start
                }
              });
            } catch (error: unknown) {
              failedCount += 1;
              await options.dependencies.leaseStore.markFailed(leaseResult.record.token, options.dependencies.clock.now(), classifyScheduleError(error));
              await emitRuntimeTelemetry(telemetry, {
                name: "runtime.dependency.observed",
                level: "error",
                at: runtimeNow(options.dependencies.clock),
                stage: "fetch",
                queue: fetchRoute.mainQueue.name,
                outcome: "failure",
                attributes: {
                  event: "scheduler.feed.failed",
                  dependency: "broker",
                  feedId: decision.feed.feedId,
                  windowStart: decision.window.start,
                  attemptCount: leaseResult.record.attemptCount,
                  error: classifyScheduleError(error)
                }
              });
            }
          }

          await emitRuntimeTelemetry(telemetry, {
            name: "runtime.dependency.observed",
            level: failedCount === 0 ? "info" : "error",
            at: checkedAt,
            stage: "fetch",
            queue: fetchRoute.mainQueue.name,
            outcome: failedCount === 0 ? "success" : "failure",
            attributes: {
              event: "scheduler.run.completed",
              dependency: "scheduler",
              dueFeedCount: selection.due.length,
              scheduledCount,
              confirmedCount,
              failedCount,
              skippedCount: selection.skipped.length,
              shadowMode: options.config.shadowMode
            }
          });

          if (failedCount === 0) {
            lastSuccessAt = runtimeNow(options.dependencies.clock);
            metrics?.setLastSuccessTimestamp(
              Math.floor(new Date(lastSuccessAt).getTime() / 1_000)
            );
          }

          return {
            checkedAt,
            dueFeedCount: selection.due.length,
            scheduledCount,
            confirmedCount,
            skippedCount: selection.skipped.length,
            failedCount,
            shadowMode: options.config.shadowMode,
            decisions: selection.decisions
          };
        });

        cycleOutcome = result.failedCount === 0 ? "success" : "failure";
        return result;
      } finally {
        observeSchedulerCycle(cycleHistograms, cycleOutcome, (performance.now() - cycleStartedAt) / 1_000);
        metrics?.setInFlight(fetchRoute.mainQueue.name, drain.inFlight);

        if (started) {
          await service.health.readiness().catch(() => undefined);
        }
      }
    },
    collectOperationalMetrics(): string {
      return collectSchedulerOperationalMetrics(
        options.config,
        options.dependencies,
        schedulingLoopActive,
        schedulingLoopIsFresh(options.config, options.dependencies, schedulingLoopActive, lastSuccessAt),
        cycleHistograms
      );
    },
    async stop(): Promise<void> {
      lifecycleGeneration += 1;

      if (!started && broker.state === "closed") {
        return;
      }

      schedulingLoopActive = false;

      drain.stopAcceptingWork();
      metrics?.setShutdownDraining(true);
      await drain.waitForDrain(options.config.shutdownTimeoutMs);
      await broker.stop("shutdown");
      await options.dependencies.leaseStore.close();
      metrics?.setShutdownDraining(false);
      started = false;
      recordRuntimeProbeMetric(metrics, options.dependencies.clock, "startup", "unhealthy");
      recordRuntimeProbeMetric(metrics, options.dependencies.clock, "readiness", "unhealthy");
    }
  };

  return service;
}

async function acquireScheduleLease(
  options: SchedulerServiceOptions,
  telemetry: RuntimeTelemetrySink | undefined,
  decision: DueFeedDecision,
  now: Date,
  checkedAt: string,
  queueName: string
): Promise<Awaited<ReturnType<SchedulerDependencies["leaseStore"]["acquire"]>> | undefined> {
  try {
    return await options.dependencies.leaseStore.acquire({
      feedId: decision.feed.feedId,
      idempotencyKey: decision.idempotencyKey,
      window: decision.window,
      now,
      leaseMs: options.config.leaseMs
    });
  } catch (error: unknown) {
    await emitRuntimeTelemetry(telemetry, {
      name: "runtime.dependency.observed",
      level: "error",
      at: checkedAt,
      stage: "fetch",
      queue: queueName,
      outcome: "failure",
      attributes: {
        event: "scheduler.feed.failed",
        dependency: "lease-store",
        feedId: decision.feed.feedId,
        windowStart: decision.window.start,
        idempotencyKey: decision.idempotencyKey,
        error: classifyScheduleError(error)
      }
    });

    return undefined;
  }
}

function createFetchPublishCommand(
  decision: DueFeedDecision,
  idFactory: SchedulerIdFactory,
  config: SchedulerConfig,
  producedAt: string
): {
  readonly envelope: WorkerMessageEnvelope;
  readonly payload: Readonly<Record<string, unknown>>;
} {
  const route = getWorkerRoute("fetch");
  const messageId = idFactory.uuid();
  const correlationId = idFactory.uuid();
  const pipelineRunId = idFactory.uuid();
  const stageExecutionId = idFactory.uuid();
  const traceparent = idFactory.traceparent();
  const payload = {
    schemaId: STAGE_PAYLOAD_SCHEMA_IDS.feedFetchRequest,
    schemaVersion: STAGE_PAYLOAD_SCHEMA_VERSION,
    pipelineRunId,
    stageExecutionId,
    sourceMessageId: messageId,
    idempotencyKey: decision.idempotencyKey,
    traceparent,
    producedAt,
    feedId: decision.feed.feedId,
    feedUrl: decision.feed.feedUrl,
    shardIndex: decision.feed.shardIndex,
    shardCount: decision.feed.shardCount,
    fetchReason: "scheduled",
    ...(decision.feed.conditional === undefined ? {} : {
      conditional: decision.feed.conditional
    }),
    limits: {
      timeoutMs: decision.feed.limits?.timeoutMs ?? 15_000,
      maxItems: decision.feed.limits?.maxItems ?? 35,
      scheduleWindowStart: decision.window.start,
      scheduleWindowEnd: decision.window.end,
      priority: decision.feed.priority
    }
  } as const;
  const payloadValidation = validateStagePayload(payload);

  if (!payloadValidation.ok) {
    throw new Error(`Invalid feed fetch payload: ${payloadValidation.issues.map((issue) => `${issue.path}:${issue.code}`).join(",")}`);
  }

  const envelope = assertWorkerEnvelope({
    schemaId: route.schemaId,
    schemaVersion: 1,
    route: "fetch",
    messageId,
    causationId: messageId,
    correlationId,
    traceparent,
    idempotencyKey: decision.idempotencyKey,
    aggregate: {
      type: "feed",
      id: decision.feed.feedId,
      version: 1
    },
    occurredAt: producedAt,
    attempt: {
      count: 1,
      max: WORKER_DELIVERY_BEHAVIOR.maxAttempts,
      firstAttemptAt: producedAt
    },
    producer: {
      name: "scheduler",
      version: config.serviceVersion,
      instanceId: config.host
    },
    payloadRef: {
      kind: "backend-record",
      uri: `backend://worker-uplift/scheduler/${encodeURIComponent(decision.idempotencyKey)}`,
      mediaType: "application/json",
      sizeBytes: getStagePayloadSizeBytes(payload)
    }
  });

  return {
    envelope,
    payload
  };
}

function classifyScheduleError(error: unknown): string {
  if (error instanceof Error && error.name.length > 0) {
    return error.name;
  }

  return "unknown-scheduler-error";
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

function brokerDependencyReadinessCheck(dependencies: SchedulerDependencies): RuntimeHealthCheck {
  return {
    name: "rabbitmq-publisher",
    critical: true,
    check: async () => {
      const probe = await dependencies.brokerProbe.probe();

      return {
        status: probe.status,
        details: {
          source: dependencies.brokerProbe.name,
          summary: probe.summary
        }
      };
    }
  };
}

function leaseStoreReadinessCheck(dependencies: SchedulerDependencies): RuntimeHealthCheck {
  return {
    name: "schedule-lease-store",
    critical: true,
    check: async () => {
      const probe = await dependencies.leaseStore.probe();

      return {
        status: probe.status,
        details: {
          source: dependencies.leaseStore.name,
          summary: probe.summary
        }
      };
    }
  };
}

function runtimeClockReadinessCheck(
  config: SchedulerConfig,
  clock: RuntimeClock
): RuntimeHealthCheck {
  return {
    name: "runtime-clock",
    critical: true,
    check: () => {
      if (config.dependencyMode !== "production") {
        return "ok";
      }

      const skewMs = Math.abs(Date.now() - clock.now().getTime());

      return skewMs <= 5_000
        ? {
            status: "ok",
            details: {
              source: "system-runtime-clock",
              freshnessBoundMs: 5_000
            }
          }
        : {
            status: "unhealthy",
            details: {
              source: "system-runtime-clock",
              reason: "clock-outside-freshness-bound",
              freshnessBoundMs: 5_000
            }
          };
    }
  };
}

function assertSchedulerDependencyBoundary(
  config: SchedulerConfig,
  dependencies: SchedulerDependencies
): void {
  if (dependencies.mode !== config.dependencyMode) {
    throw new Error("Scheduler dependency bundle does not match configured dependency mode.");
  }

  if (config.dependencyMode !== "production") {
    return;
  }

  if (
    dependencies.clockKind !== "system"
    || dependencies.clock !== SYSTEM_RUNTIME_CLOCK
    || dependencies.feedSource.adapterKind !== "backend-api"
    || dependencies.feedSource.name !== "backend-api-feed-source"
    || dependencies.leaseStore.adapterKind !== "postgres"
    || dependencies.leaseStore.name !== "postgres-schedule-lease-store"
    || dependencies.brokerKind !== "rabbitmq"
    || dependencies.brokerTransport.name !== "rabbitmq-payload-publisher"
    || dependencies.brokerProbe.name !== "rabbitmq-payload-publisher"
  ) {
    throw new Error("Production scheduler dependency boundary rejected a local or unapproved adapter.");
  }
}

function schedulingLoopReadinessCheck(
  config: SchedulerConfig,
  dependencies: SchedulerDependencies,
  isActive: () => boolean,
  latestSuccessAt: () => string | undefined
): RuntimeHealthCheck {
  return {
    name: "scheduler-loop",
    critical: true,
    check: () => config.dependencyMode !== "production" || schedulingLoopIsFresh(
      config,
      dependencies,
      isActive(),
      latestSuccessAt()
    )
      ? "ok"
      : {
          status: "unhealthy",
          details: {
            reason: !isActive()
              ? "production-scheduling-loop-inactive"
              : latestSuccessAt() === undefined
                ? "production-scheduling-loop-never-succeeded"
                : "production-scheduling-loop-stale",
            staleAfterSeconds: schedulingLoopStaleAfterMs(config) / 1_000
          }
        }
  };
}

function schedulingLoopIsFresh(
  config: SchedulerConfig,
  dependencies: SchedulerDependencies,
  active: boolean,
  latestSuccessAt: string | undefined
): boolean {
  if (!active || latestSuccessAt === undefined) {
    return false;
  }

  const ageMs = dependencies.clock.now().getTime() - new Date(latestSuccessAt).getTime();

  return Number.isFinite(ageMs) && ageMs >= 0 && ageMs <= schedulingLoopStaleAfterMs(config);
}

function schedulingLoopStaleAfterMs(config: SchedulerConfig): number {
  return config.cadenceMs * 3;
}

function productionAdaptersReadinessCheck(
  config: SchedulerConfig,
  dependencies: SchedulerDependencies
): RuntimeHealthCheck {
  return {
    name: "production-adapters",
    critical: true,
    check: () => {
      const adapter = aggregateAdapterMode(dependencies);

      return config.dependencyMode !== "production" || adapter === "production"
        ? "ok"
        : {
            status: "unhealthy",
            details: {
              expected: "production",
              actual: adapter
            }
          };
    }
  };
}

function aggregateAdapterMode(dependencies: SchedulerDependencies): "local" | "production" | "mixed" {
  const modes = [
    dependencies.clockKind === "system" ? "production" : "local",
    dependencies.feedSource.adapterKind === "backend-api" ? "production" : "local",
    dependencies.leaseStore.adapterKind === "postgres" ? "production" : "local",
    dependencies.brokerKind === "rabbitmq" ? "production" : "local"
  ] as const;
  const unique = new Set(modes);

  if (unique.size === 1) {
    return modes[0];
  }

  return "mixed";
}

function collectSchedulerOperationalMetrics(
  config: SchedulerConfig,
  dependencies: SchedulerDependencies,
  loopActive: boolean,
  loopFresh: boolean,
  cycleHistograms: ReadonlyMap<SchedulerCycleOutcome, SchedulerCycleHistogram>
): string {
  const identity = {
    environment: config.environment,
    service: config.serviceName
  };
  const lines = [
    "# HELP nutsnews_worker_scheduler_loop_active Whether the scheduler loop is active.",
    "# TYPE nutsnews_worker_scheduler_loop_active gauge",
    metricLine("nutsnews_worker_scheduler_loop_active", identity, loopActive ? 1 : 0),
    "# HELP nutsnews_worker_scheduler_loop_fresh Whether the active scheduler loop completed successfully within three cadences.",
    "# TYPE nutsnews_worker_scheduler_loop_fresh gauge",
    metricLine("nutsnews_worker_scheduler_loop_fresh", identity, loopFresh ? 1 : 0),
    ...collectSchedulerCycleMetrics(identity, cycleHistograms)
  ];

  return `${lines.join("\n")}\n`;
}

function observeSchedulerCycle(
  histograms: Map<SchedulerCycleOutcome, SchedulerCycleHistogram>,
  outcome: SchedulerCycleOutcome,
  valueSeconds: number
): void {
  const value = Number.isFinite(valueSeconds) ? Math.max(0, valueSeconds) : 0;
  const histogram = histograms.get(outcome) ?? {
    buckets: SCHEDULER_CYCLE_DURATION_BUCKETS_SECONDS.map(() => 0),
    count: 0,
    sum: 0
  };

  histogram.count += 1;
  histogram.sum += value;

  for (const [index, boundary] of SCHEDULER_CYCLE_DURATION_BUCKETS_SECONDS.entries()) {
    if (value <= boundary) {
      histogram.buckets[index] = (histogram.buckets[index] ?? 0) + 1;
    }
  }

  histograms.set(outcome, histogram);
}

function collectSchedulerCycleMetrics(
  identity: Readonly<Record<string, string>>,
  histograms: ReadonlyMap<SchedulerCycleOutcome, SchedulerCycleHistogram>
): string[] {
  if (histograms.size === 0) {
    return [];
  }

  const lines = [
    "# HELP nutsnews_worker_scheduler_cycle_duration_seconds Scheduler cycle duration by terminal outcome.",
    "# TYPE nutsnews_worker_scheduler_cycle_duration_seconds histogram"
  ];

  for (const outcome of ["success", "failure"] as const) {
    const histogram = histograms.get(outcome);

    if (histogram === undefined) {
      continue;
    }

    for (const [index, boundary] of SCHEDULER_CYCLE_DURATION_BUCKETS_SECONDS.entries()) {
      lines.push(metricLine("nutsnews_worker_scheduler_cycle_duration_seconds_bucket", {
        ...identity,
        outcome,
        le: String(boundary)
      }, histogram.buckets[index] ?? 0));
    }

    lines.push(
      metricLine("nutsnews_worker_scheduler_cycle_duration_seconds_bucket", {
        ...identity,
        outcome,
        le: "+Inf"
      }, histogram.count),
      metricLine("nutsnews_worker_scheduler_cycle_duration_seconds_sum", {
        ...identity,
        outcome
      }, histogram.sum),
      metricLine("nutsnews_worker_scheduler_cycle_duration_seconds_count", {
        ...identity,
        outcome
      }, histogram.count)
    );
  }

  return lines;
}

function recordRuntimeProbeMetric(
  metrics: SchedulerMetricsSink | undefined,
  clock: RuntimeClock,
  probe: RuntimeHealthProbe,
  status: RuntimeHealthStatus
): void {
  void metrics?.emit({
    name: "runtime.health.evaluated",
    level: status === "ok" ? "info" : "warn",
    at: runtimeNow(clock),
    outcome: status,
    attributes: {
      probe,
      status,
      checkCount: 0,
      checks: []
    }
  });
}

function metricLine(
  name: string,
  labels: Readonly<Record<string, string>>,
  value: number
): string {
  const rendered = Object.entries(labels)
    .map(([key, label]) => `${key}="${escapeMetricLabel(boundedMetricLabel(label))}"`)
    .join(",");

  return `${name}{${rendered}} ${formatMetricNumber(value)}`;
}

function boundedMetricLabel(value: string): string {
  const bounded = value
    .trim()
    .replace(/[^A-Za-z0-9_.:/@+-]+/gu, "_")
    .slice(0, 128);

  return bounded.length > 0 ? bounded : "unknown";
}

function formatMetricNumber(value: number): string {
  return Number.isInteger(value) ? String(value) : String(Number(value.toFixed(9)));
}

function escapeMetricLabel(value: string): string {
  return value
    .replaceAll("\\", "\\\\")
    .replaceAll("\n", "\\n")
    .replaceAll('"', '\\"');
}

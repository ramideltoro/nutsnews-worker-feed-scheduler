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
  type PrometheusRuntimeTelemetrySink,
  type RuntimeHealthCheck,
  type RuntimeHealthProbeSet,
  type RuntimeClock,
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

export interface SchedulerServiceOptions {
  readonly config: SchedulerConfig;
  readonly dependencies: SchedulerDependencies;
  readonly idFactory?: SchedulerIdFactory;
  readonly telemetry?: RuntimeTelemetrySink;
  readonly metrics?: PrometheusRuntimeTelemetrySink;
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
  start(): Promise<void>;
  runOnce(): Promise<SchedulerRunOnceResult>;
  stop(): Promise<void>;
}

export function createSchedulerService(options: SchedulerServiceOptions): SchedulerService {
  assertSchedulerDependencyBoundary(options.config, options.dependencies);
  const fetchRoute = getWorkerRoute("fetch");
  const idFactory = options.idFactory ?? createCryptoSchedulerIdFactory();
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
          brokerDependencyReadinessCheck(options.dependencies),
          feedSourceReadinessCheck(options.dependencies),
          leaseStoreReadinessCheck(options.dependencies),
          runtimeClockReadinessCheck(options.config, options.dependencies.clock),
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
        const now = options.dependencies.clock.now();
        const feeds = await options.dependencies.feedSource.listActiveFeeds(now);
        const selection = selectDueFeeds(feeds, now, feeds.length);
        let scheduledCount = 0;
        let confirmedCount = 0;
        let failedCount = 0;
        let attemptedCount = 0;

        for (const decision of selection.skipped) {
          await emitRuntimeTelemetry(options.telemetry, {
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

          const leaseResult = await acquireScheduleLease(options, decision, now, checkedAt, fetchRoute.mainQueue.name);

          if (leaseResult === undefined) {
            attemptedCount += 1;
            failedCount += 1;
            continue;
          }

          if (leaseResult.status !== "acquired") {
            await emitRuntimeTelemetry(options.telemetry, {
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
          await emitRuntimeTelemetry(options.telemetry, {
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
            await emitRuntimeTelemetry(options.telemetry, {
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
            await emitRuntimeTelemetry(options.telemetry, {
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

        await emitRuntimeTelemetry(options.telemetry, {
          name: "runtime.dependency.observed",
          level: "info",
          at: checkedAt,
          stage: "fetch",
          queue: fetchRoute.mainQueue.name,
          outcome: "success",
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

        options.metrics?.recordDependencyLatency(fetchRoute.mainQueue.name, 0, "success");
        options.metrics?.setInFlight(fetchRoute.mainQueue.name, drain.inFlight);

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
    },
    async stop(): Promise<void> {
      if (!started && broker.state === "closed") {
        return;
      }

      drain.stopAcceptingWork();
      options.metrics?.setShutdownDraining(true);
      await drain.waitForDrain(options.config.shutdownTimeoutMs);
      await broker.stop("shutdown");
      await options.dependencies.leaseStore.close();
      options.metrics?.setShutdownDraining(false);
      started = false;
    }
  } satisfies SchedulerService;

  return service;
}

async function acquireScheduleLease(
  options: SchedulerServiceOptions,
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
    await emitRuntimeTelemetry(options.telemetry, {
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

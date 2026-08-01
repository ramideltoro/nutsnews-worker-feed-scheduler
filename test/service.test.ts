import {
  createBufferedRuntimeTelemetrySink,
  createPrometheusRuntimeTelemetrySink,
  SYSTEM_RUNTIME_CLOCK
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  describe,
  expect,
  it,
  vi
} from "vitest";

import { loadSchedulerConfig } from "../src/config.js";
import type { SchedulerDependencies } from "../src/dependencies.js";
import type { ScheduleLeaseStore } from "../src/lease-store.js";
import { createSchedulerLoop } from "../src/loop.js";
import { createSchedulerService } from "../src/service.js";
import {
  LocalBrokerTransport,
  createLocalSchedulerDependencies
} from "../src/test-doubles.js";

describe("createSchedulerService", () => {
  it("exports explicit probe states before startup and transitions them with the service lifecycle", async () => {
    const config = loadSchedulerConfig({
      NUTSNEWS_SCHEDULER_TELEMETRY_LOGS: "silent"
    });
    const service = createSchedulerService({
      config,
      dependencies: createLocalSchedulerDependencies()
    });
    const initial = service.collectOperationalMetrics();

    expect(initial).toContain('nutsnews_worker_health_probe{environment="local",service="nutsnews-worker-feed-scheduler",probe="liveness",outcome="ok"} 1');
    expect(initial).toContain('nutsnews_worker_health_probe{environment="local",service="nutsnews-worker-feed-scheduler",probe="startup",outcome="unhealthy"} 1');
    expect(initial).toContain('nutsnews_worker_health_probe{environment="local",service="nutsnews-worker-feed-scheduler",probe="readiness",outcome="unhealthy"} 1');
    expect(initial.match(/^# TYPE nutsnews_worker_health_probe gauge$/gmu)).toHaveLength(1);
    expect(initial).not.toMatch(/^# TYPE nutsnews_worker_health gauge$/mu);

    await service.start();
    expect(service.collectOperationalMetrics()).toContain('probe="startup",outcome="ok"} 1');

    await service.startScheduling();
    expect(service.collectOperationalMetrics()).toContain('probe="readiness",outcome="ok"} 1');

    await service.stop();
    const stopped = service.collectOperationalMetrics();
    expect(stopped).toContain('probe="startup",outcome="unhealthy"} 1');
    expect(stopped).toContain('probe="readiness",outcome="unhealthy"} 1');
  });

  it("starts, becomes ready, records a dry scheduler check, and drains cleanly", async () => {
    const config = loadSchedulerConfig({
      NUTSNEWS_SCHEDULER_HTTP_PORT: "0",
      NUTSNEWS_SCHEDULER_TELEMETRY_LOGS: "silent"
    });
    const dependencies = createLocalSchedulerDependencies({
      dueFeedCount: 2
    });
    const telemetry = createBufferedRuntimeTelemetrySink();
    const metrics = createPrometheusRuntimeTelemetrySink({
      identity: {
        service: config.serviceName,
        version: config.serviceVersion,
        environment: config.environment,
        host: config.host
      }
    });
    const service = createSchedulerService({
      config,
      dependencies,
      telemetry,
      metrics
    });

    await service.start();

    expect(service.isStarted).toBe(true);
    expect((await service.health.liveness()).status).toBe("ok");
    expect((await service.health.startup()).status).toBe("ok");
    expect((await service.health.readiness()).status).toBe("ok");

    await expect(service.runOnce()).resolves.toMatchObject({
      dueFeedCount: 2,
      shadowMode: true
    });
    expect(service.lastSuccessAt).toBeDefined();
    expect(service.collectOperationalMetrics()).toContain("nutsnews_worker_build_info");
    expect(service.collectOperationalMetrics()).toContain("nutsnews_worker_expected_active");
    expect(service.collectOperationalMetrics()).toContain("nutsnews_worker_last_success_timestamp_seconds");
    expect(service.collectOperationalMetrics()).toContain("nutsnews_worker_scheduler_cycle_duration_seconds_bucket");
    expect(service.collectOperationalMetrics()).toMatch(/nutsnews_worker_scheduler_cycle_duration_seconds_count\{[^\n]+outcome="success"\} 1/u);
    expect(metrics.collect()).toMatch(/nutsnews_worker_inflight\{[^\n]+\} 0/u);
    expect(metrics.collect()).not.toContain("nutsnews_worker_dependency_duration_ms");

    await service.stop();

    expect(service.isStarted).toBe(false);
    expect(service.broker.state).toBe("closed");
    expect(telemetry.events.some((event) => event.name === "runtime.broker.state_changed")).toBe(true);
  });

  it("reports readiness unhealthy when the local feed source is unhealthy", async () => {
    const config = loadSchedulerConfig({
      NUTSNEWS_SCHEDULER_TELEMETRY_LOGS: "silent"
    });
    const service = createSchedulerService({
      config,
      dependencies: createLocalSchedulerDependencies({
        status: "unhealthy"
      })
    });

    await service.start();

    expect((await service.health.readiness()).status).toBe("unhealthy");

    await service.stop();
  });

  it("closes the scheduler-owned lease store during shutdown", async () => {
    const config = loadSchedulerConfig({
      NUTSNEWS_SCHEDULER_TELEMETRY_LOGS: "silent"
    });
    const dependencies = createLocalSchedulerDependencies();
    const close = vi.spyOn(dependencies.leaseStore, "close");
    const service = createSchedulerService({
      config,
      dependencies
    });

    await service.start();
    await service.stop();

    expect(close).toHaveBeenCalledOnce();
  });

  it("runs an immediate non-overlapping scheduling loop and cancels its timer on shutdown", async () => {
    vi.useFakeTimers();

    try {
      const config = loadSchedulerConfig({
        NUTSNEWS_SCHEDULER_CADENCE_MS: "1000",
        NUTSNEWS_SCHEDULER_LEASE_MS: "2000",
        NUTSNEWS_SCHEDULER_TELEMETRY_LOGS: "silent"
      });
      const dependencies = createLocalSchedulerDependencies();
      let listCalls = 0;
      const service = createSchedulerService({
        config,
        dependencies: {
          ...dependencies,
          feedSource: {
            ...dependencies.feedSource,
            listActiveFeeds: (now) => {
              listCalls += 1;
              return dependencies.feedSource.listActiveFeeds(now);
            }
          }
        }
      });

      await service.start();
      const loop = createSchedulerLoop({
        service,
        cadenceMs: config.cadenceMs
      });

      loop.start();
      await vi.advanceTimersByTimeAsync(0);

      expect(service.isSchedulingLoopActive).toBe(true);
      expect(listCalls).toBe(1);
      expect(service.collectOperationalMetrics()).toContain("nutsnews_worker_scheduler_loop_active");

      await vi.advanceTimersByTimeAsync(1_000);
      expect(listCalls).toBe(2);

      await loop.stop();
      await service.stop();
      await vi.advanceTimersByTimeAsync(2_000);
      expect(listCalls).toBe(2);
      expect(service.isSchedulingLoopActive).toBe(false);
    } finally {
      vi.useRealTimers();
    }
  });

  it("rejects local adapters before a production scheduler can start", () => {
    const config = loadSchedulerConfig({
      NUTSNEWS_SCHEDULER_DEPENDENCY_MODE: "production",
      NUTSNEWS_SCHEDULER_BUILD_REVISION: "0123456789abcdef0123456789abcdef01234567",
      NUTSNEWS_SCHEDULER_DATABASE_URL: "postgres://configured",
      NUTSNEWS_SCHEDULER_BACKEND_API_URL: "https://backend.example.test",
      NUTSNEWS_SCHEDULER_BACKEND_API_TOKEN: "configured",
      NUTSNEWS_SCHEDULER_RABBITMQ_URL: "amqps://configured",
      NUTSNEWS_SCHEDULER_TELEMETRY_LOGS: "silent"
    });
    expect(() => createSchedulerService({
      config,
      dependencies: {
        ...createLocalSchedulerDependencies(),
        mode: "production"
      }
    })).toThrow(/rejected a local or unapproved adapter/u);
  });

  it("treats shadow ownership as paging metadata while adapters and loop determine readiness", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-07-23T00:00:00.000Z"));

    try {
    const config = loadSchedulerConfig({
      NUTSNEWS_SCHEDULER_DEPENDENCY_MODE: "production",
      NUTSNEWS_SCHEDULER_BUILD_REVISION: "0123456789abcdef0123456789abcdef01234567",
      NUTSNEWS_SCHEDULER_DATABASE_URL: "postgres://configured",
      NUTSNEWS_SCHEDULER_BACKEND_API_URL: "https://backend.example.test",
      NUTSNEWS_SCHEDULER_BACKEND_API_TOKEN: "configured",
      NUTSNEWS_SCHEDULER_RABBITMQ_URL: "amqps://configured",
      NUTSNEWS_SCHEDULER_TELEMETRY_LOGS: "silent"
    });
    const dependencies = productionCompatibleTestDependencies();
    const service = createSchedulerService({
      config,
      dependencies
    });

    await service.start();
    expect((await service.health.readiness()).status).toBe("unhealthy");

    await service.startScheduling();
    const readiness = await service.health.readiness();

    expect(readiness.status).toBe("ok");
    expect(readiness.checks.map((check) => check.name)).toEqual([
      "broker-lifecycle",
      "rabbitmq-publisher",
      "feed-source",
      "schedule-lease-store",
      "runtime-clock",
      "scheduler-loop",
      "production-adapters"
    ]);
    expect(service.collectOperationalMetrics()).toMatch(/nutsnews_worker_expected_active\{[^\n]+\} 0/u);
    expect(service.collectOperationalMetrics()).toMatch(/nutsnews_worker_scheduler_loop_active\{[^\n]+\} 1/u);
    expect(service.collectOperationalMetrics()).toMatch(/nutsnews_worker_scheduler_loop_fresh\{[^\n]+\} 1/u);

    vi.setSystemTime(new Date(Date.now() + config.cadenceMs * 3 + 1));
    expect((await service.health.readiness()).status).toBe("unhealthy");
    expect(service.collectOperationalMetrics()).toMatch(/nutsnews_worker_scheduler_loop_active\{[^\n]+\} 1/u);
    expect(service.collectOperationalMetrics()).toMatch(/nutsnews_worker_scheduler_loop_fresh\{[^\n]+\} 0/u);
    expect(service.collectOperationalMetrics()).toContain('probe="readiness",outcome="unhealthy"} 1');

    await service.runOnce();
    expect(service.collectOperationalMetrics()).toMatch(/nutsnews_worker_scheduler_loop_fresh\{[^\n]+\} 1/u);
    expect(service.collectOperationalMetrics()).toContain('probe="readiness",outcome="ok"} 1');

    await service.stop();
    } finally {
      vi.useRealTimers();
    }
  });

  it("requires production adapters and a recent loop when a protected cutover becomes active", async () => {
    const shadowConfig = loadSchedulerConfig({
      NUTSNEWS_SCHEDULER_DEPENDENCY_MODE: "production",
      NUTSNEWS_SCHEDULER_BUILD_REVISION: "0123456789abcdef0123456789abcdef01234567",
      NUTSNEWS_SCHEDULER_DATABASE_URL: "postgres://configured",
      NUTSNEWS_SCHEDULER_BACKEND_API_URL: "https://backend.example.test",
      NUTSNEWS_SCHEDULER_BACKEND_API_TOKEN: "configured",
      NUTSNEWS_SCHEDULER_RABBITMQ_URL: "amqps://configured",
      NUTSNEWS_SCHEDULER_TELEMETRY_LOGS: "silent"
    });
    const config = {
      ...shadowConfig,
      shadowMode: false
    };
    const service = createSchedulerService({
      config,
      dependencies: productionCompatibleTestDependencies()
    });

    await service.start();
    expect((await service.health.readiness()).status).toBe("unhealthy");

    await service.startScheduling();
    expect((await service.health.readiness()).status).toBe("ok");
    expect(service.collectOperationalMetrics()).toContain('deployment="production",adapter="production"');
    expect(service.collectOperationalMetrics()).toMatch(/nutsnews_worker_expected_active\{[^\n]+\} 1/u);

    await service.stop();
  });

  it("reschedules after a failed iteration", async () => {
    vi.useFakeTimers();

    try {
      const config = loadSchedulerConfig({
        NUTSNEWS_SCHEDULER_CADENCE_MS: "1000",
        NUTSNEWS_SCHEDULER_LEASE_MS: "2000",
        NUTSNEWS_SCHEDULER_TELEMETRY_LOGS: "silent"
      });
      const dependencies = createLocalSchedulerDependencies();
      let listCalls = 0;
      const service = createSchedulerService({
        config,
        dependencies: {
          ...dependencies,
          feedSource: {
            ...dependencies.feedSource,
            listActiveFeeds: () => {
              listCalls += 1;
              throw new Error("feed source unavailable");
            }
          }
        }
      });

      await service.start();
      const loop = createSchedulerLoop({
        service,
        cadenceMs: config.cadenceMs
      });

      loop.start();
      await vi.advanceTimersByTimeAsync(0);
      expect(listCalls).toBe(1);

      await vi.advanceTimersByTimeAsync(1_000);
      expect(listCalls).toBe(2);
      expect(service.isSchedulingLoopActive).toBe(true);

      await loop.stop();
      await service.stop();
    } finally {
      vi.useRealTimers();
    }
  });

  it("keeps scheduling and lifecycle state independent from rejecting telemetry and metric sinks", async () => {
    const config = loadSchedulerConfig({
      NUTSNEWS_SCHEDULER_TELEMETRY_LOGS: "silent"
    });
    const realMetrics = createPrometheusRuntimeTelemetrySink({
      identity: {
        service: config.serviceName,
        version: config.serviceVersion,
        environment: config.environment,
        host: config.host
      }
    });
    const rejectingMetrics = {
      ...realMetrics,
      allowedLabels: realMetrics.allowedLabels,
      emit: () => Promise.reject(new Error("metric emit unavailable")),
      collect: () => {
        throw new Error("metric collect unavailable");
      },
      setInFlight: () => {
        throw new Error("metric gauge unavailable");
      },
      setShutdownDraining: () => {
        throw new Error("metric drain unavailable");
      }
    };
    const service = createSchedulerService({
      config,
      dependencies: createLocalSchedulerDependencies({
        dueFeedCount: 1
      }),
      telemetry: {
        emit: () => Promise.reject(new Error("log unavailable"))
      },
      metrics: rejectingMetrics
    });

    await expect(service.start()).resolves.toBeUndefined();
    await expect(service.startScheduling()).resolves.toBeUndefined();
    expect(service.isSchedulingLoopActive).toBe(true);
    expect(service.lastSuccessAt).toBeDefined();
    await expect(service.stop()).resolves.toBeUndefined();
    expect(service.isStarted).toBe(false);
  });
});

function productionCompatibleTestDependencies(): SchedulerDependencies {
  const local = createLocalSchedulerDependencies();
  const leaseStore = productionIdentityLeaseStore(local.leaseStore);
  const broker = new ProductionIdentityLocalBroker();

  return {
    mode: "production",
    clockKind: "system",
    brokerKind: "rabbitmq",
    clock: SYSTEM_RUNTIME_CLOCK,
    feedSource: {
      ...local.feedSource,
      name: "backend-api-feed-source",
      adapterKind: "backend-api"
    },
    leaseStore,
    brokerTransport: broker,
    brokerProbe: broker
  };
}

function productionIdentityLeaseStore(delegate: ScheduleLeaseStore): ScheduleLeaseStore {
  return {
    name: "postgres-schedule-lease-store",
    adapterKind: "postgres",
    probe: () => delegate.probe(),
    acquire: (command) => delegate.acquire(command),
    markConfirmed: (token, confirmedAt, messageId) => delegate.markConfirmed(token, confirmedAt, messageId),
    markFailed: (token, failedAt, reason) => delegate.markFailed(token, failedAt, reason),
    get: (idempotencyKey) => delegate.get(idempotencyKey),
    close: () => delegate.close()
  };
}

class ProductionIdentityLocalBroker extends LocalBrokerTransport {
  override readonly name = "rabbitmq-payload-publisher";
}

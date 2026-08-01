import {
  SYSTEM_RUNTIME_CLOCK,
  type BrokerDeliveryHandler,
  type BrokerPublishCommand,
  type BrokerPublishReceipt,
  type RuntimeBrokerTransport
} from "@ramideltoro/nutsnews-worker-runtime";
import type {
  WorkerRoute,
  WorkerStage
} from "@ramideltoro/nutsnews-worker-contracts";
import { getWorkerRoute } from "@ramideltoro/nutsnews-worker-contracts";
import {
  afterEach,
  describe,
  expect,
  it,
  vi
} from "vitest";

import { loadSchedulerConfig } from "../src/config.js";
import type {
  SchedulerDependencies,
  SchedulerFeedSource
} from "../src/dependencies.js";
import type {
  ScheduleLeaseAcquireCommand,
  ScheduleLeaseAcquireResult,
  ScheduleLeaseRecord,
  ScheduleLeaseStore
} from "../src/lease-store.js";
import {
  BackendApiFeedSource,
  createProductionSchedulerDependencies
} from "../src/production-dependencies.js";
import { createSchedulerService } from "../src/service.js";
import {
  InMemoryScheduleLeaseStore
} from "../src/lease-store.js";
import {
  LocalBrokerTransport,
  ManualSchedulerClock,
  createLocalFeedSource,
  createLocalSchedulerDependencies
} from "../src/test-doubles.js";
import type { SchedulerFeedDefinition } from "../src/scheduling.js";

const productionEnvironment = {
  NUTSNEWS_SCHEDULER_DATABASE_URL: "postgres://scheduler.example.test/shadow",
  NUTSNEWS_SCHEDULER_BACKEND_API_URL: "https://backend.example.test",
  NUTSNEWS_SCHEDULER_BACKEND_API_TOKEN: "configured-test-token",
  NUTSNEWS_SCHEDULER_RABBITMQ_URL: "amqps://scheduler.example.test/worker-uplift"
} as const;

afterEach(() => {
  vi.useRealTimers();
});

describe("production scheduler dependencies", () => {
  it("selects only system/backend/PostgreSQL/RabbitMQ adapters in production mode", async () => {
    const config = productionConfig();
    const dependencies = createProductionSchedulerDependencies(config, productionEnvironment);

    expect(dependencies).toMatchObject({
      mode: "production",
      clockKind: "system",
      brokerKind: "rabbitmq"
    });
    expect(dependencies.clock).toBe(SYSTEM_RUNTIME_CLOCK);
    expect(dependencies.clock).not.toBeInstanceOf(ManualSchedulerClock);
    expect(dependencies.feedSource).not.toBe(createLocalFeedSource());
    expect(dependencies.feedSource).toMatchObject({
      name: "backend-api-feed-source",
      adapterKind: "backend-api"
    });
    expect(dependencies.leaseStore).not.toBeInstanceOf(InMemoryScheduleLeaseStore);
    expect(dependencies.leaseStore).toMatchObject({
      name: "postgres-schedule-lease-store",
      adapterKind: "postgres"
    });
    expect(dependencies.brokerTransport).not.toBeInstanceOf(LocalBrokerTransport);
    expect(dependencies.brokerTransport.name).toBe("rabbitmq-payload-publisher");

    await dependencies.leaseStore.close();
  });

  it("rejects a local dependency bundle relabeled as production", () => {
    const config = productionConfig();
    const local = createLocalSchedulerDependencies();

    expect(() => createSchedulerService({
      config,
      dependencies: {
        ...local,
        mode: "production"
      }
    })).toThrow(/rejected a local or unapproved adapter/u);
  });

  it("uses a current system readiness timestamp and names every real dependency", async () => {
    const config = productionConfig();
    const dependencies = productionTestDependencies();
    const service = createSchedulerService({
      config,
      dependencies
    });

    await service.start();
    expect((await service.health.readiness()).status).toBe("unhealthy");

    await service.startScheduling();
    const readiness = await service.health.readiness();
    const readinessJson = JSON.stringify(readiness);
    const checkedAtMs = Date.parse(readiness.checkedAt);

    expect(readiness.status).toBe("ok");
    expect(Math.abs(Date.now() - checkedAtMs)).toBeLessThanOrEqual(5_000);
    expect(readinessJson).toContain("backend-api-feed-source");
    expect(readinessJson).toContain("postgres-schedule-lease-store");
    expect(readinessJson).toContain("rabbitmq-payload-publisher");
    expect(readiness.checks.map((check) => check.name)).toEqual([
      "broker-lifecycle",
      "rabbitmq-publisher",
      "feed-source",
      "schedule-lease-store",
      "scheduler-loop",
      "production-adapters"
    ]);
    expect(readinessJson).not.toContain("local-feed-source");
    expect(readinessJson).not.toContain("local test");

    await service.stop();
  });

  it("fails startup closed before broker connection when a required adapter is unavailable", async () => {
    const config = productionConfig();
    const dependencies = productionTestDependencies("unhealthy");
    const broker = dependencies.brokerTransport as ProductionTestBrokerTransport;
    const service = createSchedulerService({
      config,
      dependencies
    });

    await expect(service.start()).rejects.toThrow(/failed the startup probe/u);
    expect(broker.connectCount).toBe(0);
  });

  it("leaves the lease fenced when publication crosses its production deadline", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-07-23T00:05:42.000Z"));
    const leaseStore = new DeadlineProductionLeaseStore();
    const broker = new DeadlineProductionBrokerTransport();
    const dependencies = deadlineProductionDependencies(leaseStore, broker);
    const service = createSchedulerService({
      config: productionConfig({
        NUTSNEWS_SCHEDULER_CADENCE_MS: "60000",
        NUTSNEWS_SCHEDULER_LEASE_MS: "60000"
      }),
      dependencies
    });

    await service.start();
    await service.setSchedulingLoopActive?.(true);
    const run = service.runOnce();
    await broker.publishStarted.promise;
    await vi.advanceTimersByTimeAsync(50_000);

    await expect(run).resolves.toMatchObject({
      scheduledCount: 1,
      confirmedCount: 0,
      failedCount: 1
    });
    await expect(leaseStore.get(
      "scheduler:feed:feed-production:20260723t000500000z"
    )).resolves.toMatchObject({
      status: "leased"
    });

    await broker.finishPublish();
    await service.stop();
  });

  it("refuses publication when renewal lacks the configured server-clock duration", async () => {
    const leaseStore = new ShortRenewalProductionLeaseStore();
    const broker = new DeadlineProductionBrokerTransport();
    const service = createSchedulerService({
      config: productionConfig({
        NUTSNEWS_SCHEDULER_CADENCE_MS: "60000",
        NUTSNEWS_SCHEDULER_LEASE_MS: "60000"
      }),
      dependencies: deadlineProductionDependencies(leaseStore, broker)
    });

    await service.start();
    await service.setSchedulingLoopActive?.(true);
    await expect(service.runOnce()).resolves.toMatchObject({
      scheduledCount: 1,
      confirmedCount: 0,
      failedCount: 1
    });
    expect(broker.publishCount).toBe(0);

    await service.stop();
  });
});

describe("backend API feed source", () => {
  it("loads the approved read operation and maps active feeds without exposing credentials", async () => {
    const request = vi.fn<typeof fetch>((_input, init) => {
      expect(init?.method).toBe("POST");
      expect(typeof init?.body).toBe("string");
      expect(JSON.parse(typeof init?.body === "string" ? init.body : "{}") as unknown).toEqual({
        providerMode: "backend_postgres_primary",
        feedsPerShard: 500,
        offset: 0
      });

      return Promise.resolve(new Response(JSON.stringify([
        {
          source: "wire-service",
          url: "https://feeds.example.test/news.xml",
          is_positive_source: true
        }
      ]), {
        status: 200,
        headers: {
          "content-type": "application/json"
        }
      }));
    });
    const source = new BackendApiFeedSource({
      baseUrl: "https://backend.example.test",
      token: "not-a-real-token",
      cadenceMs: 300_000,
      request
    });

    const feeds = await source.listActiveFeeds(new Date());

    expect(request).toHaveBeenCalledOnce();
    expect(requestUrl(request.mock.calls[0]?.[0])).toBe(
      "https://backend.example.test/api/worker/db/load-feeds-for-shard"
    );
    expect(feeds).toEqual([
      expect.objectContaining({
        feedId: expect.stringMatching(/^feed-[0-9a-f]{32}$/u) as unknown,
        feedUrl: "https://feeds.example.test/news.xml",
        enabled: true,
        cadenceMs: 300_000,
        priority: 2,
        shardIndex: 0,
        shardCount: 1
      })
    ]);
    expect(JSON.stringify(feeds)).not.toContain("not-a-real-token");
  });

  it("returns value-free unhealthy readiness when the API fails", async () => {
    const source = new BackendApiFeedSource({
      baseUrl: "https://backend.example.test",
      token: "not-a-real-token",
      cadenceMs: 300_000,
      request: () => Promise.resolve(new Response("secret response body", {
        status: 503
      }))
    });

    const probe = await source.probe();

    expect(probe.status).toBe("unhealthy");
    expect(probe.summary).toBe("backend API feed source unavailable (SchedulerDependencyError)");
    expect(probe.summary).not.toContain("secret response body");
    expect(probe.summary).not.toContain("not-a-real-token");
  });
});

function productionConfig(overrides: NodeJS.ProcessEnv = {}) {
  return loadSchedulerConfig({
    NUTSNEWS_ENVIRONMENT: "production",
    NUTSNEWS_SCHEDULER_DEPENDENCY_MODE: "production",
    NUTSNEWS_SCHEDULER_BUILD_REVISION: "0123456789abcdef0123456789abcdef01234567",
    ...productionEnvironment,
    NUTSNEWS_SCHEDULER_CADENCE_MS: "300000",
    NUTSNEWS_SCHEDULER_LEASE_MS: "300000",
    NUTSNEWS_SCHEDULER_CONCURRENCY: "1",
    NUTSNEWS_SCHEDULER_SHADOW_MODE: "true",
    NUTSNEWS_SCHEDULER_TELEMETRY_LOGS: "silent",
    ...overrides
  });
}

function deadlineProductionDependencies(
  leaseStore: ScheduleLeaseStore,
  broker: DeadlineProductionBrokerTransport
): SchedulerDependencies {
  return {
    mode: "production",
    clockKind: "system",
    brokerKind: "rabbitmq",
    clock: SYSTEM_RUNTIME_CLOCK,
    feedSource: new ProductionTestFeedSource("ok", [
      {
        feedId: "feed-production",
        feedUrl: "https://feeds.example.test/production.xml",
        enabled: true,
        cadenceMs: 60_000,
        priority: 1,
        shardIndex: 0,
        shardCount: 1
      }
    ]),
    leaseStore,
    brokerTransport: broker,
    brokerProbe: broker
  };
}

function productionTestDependencies(
  feedStatus: "ok" | "unhealthy" = "ok"
): SchedulerDependencies {
  return {
    mode: "production",
    clockKind: "system",
    brokerKind: "rabbitmq",
    clock: SYSTEM_RUNTIME_CLOCK,
    feedSource: new ProductionTestFeedSource(feedStatus),
    leaseStore: new ProductionTestLeaseStore(),
    brokerTransport: new ProductionTestBrokerTransport(),
    brokerProbe: new ProductionTestBrokerTransport()
  };
}

class ProductionTestFeedSource implements SchedulerFeedSource {
  readonly name = "backend-api-feed-source";
  readonly adapterKind = "backend-api" as const;

  constructor(
    private readonly status: "ok" | "unhealthy",
    private readonly feeds: readonly SchedulerFeedDefinition[] = []
  ) {}

  probe() {
    return {
      status: this.status,
      summary: this.status === "ok"
        ? "backend API feed source ready"
        : "backend API feed source unavailable (TestError)"
    } as const;
  }

  listActiveFeeds(): readonly SchedulerFeedDefinition[] {
    return this.feeds;
  }

  countDueFeeds(): number {
    return this.feeds.length;
  }
}

class DeadlineProductionLeaseStore implements ScheduleLeaseStore {
  readonly name = "postgres-schedule-lease-store";
  readonly adapterKind = "postgres" as const;
  private readonly delegate = new InMemoryScheduleLeaseStore(
    () => SYSTEM_RUNTIME_CLOCK.now()
  );

  probe() {
    return this.delegate.probe();
  }

  acquire(command: ScheduleLeaseAcquireCommand) {
    return this.delegate.acquire(command);
  }

  renew(token: string, leaseMs: number) {
    return this.delegate.renew(token, leaseMs);
  }

  release(token: string, releasedAt: Date) {
    return this.delegate.release(token, releasedAt);
  }

  markConfirmed(token: string, confirmedAt: Date, messageId: string) {
    return this.delegate.markConfirmed(token, confirmedAt, messageId);
  }

  markFailed(token: string, failedAt: Date, reason: string) {
    return this.delegate.markFailed(token, failedAt, reason);
  }

  get(idempotencyKey: string) {
    return this.delegate.get(idempotencyKey);
  }

  close() {
    return this.delegate.close();
  }
}

class ShortRenewalProductionLeaseStore extends DeadlineProductionLeaseStore {
  override async renew(token: string, leaseMs: number): Promise<ScheduleLeaseRecord> {
    const renewed = await super.renew(token, leaseMs);
    const checkedAtMs = Date.parse(renewed.ownershipCheckedAt ?? "");

    return {
      ...renewed,
      leaseExpiresAt: new Date(checkedAtMs + 30_000).toISOString()
    };
  }
}

class ProductionTestLeaseStore implements ScheduleLeaseStore {
  readonly name = "postgres-schedule-lease-store";
  readonly adapterKind = "postgres" as const;

  probe() {
    return Promise.resolve({
      status: "ok" as const,
      summary: "PostgreSQL schedule lease store ready"
    });
  }

  acquire(_command: ScheduleLeaseAcquireCommand): Promise<ScheduleLeaseAcquireResult> {
    void _command;
    return Promise.reject(new Error("not used"));
  }

  markConfirmed(): Promise<ScheduleLeaseRecord> {
    return Promise.reject(new Error("not used"));
  }

  renew(): Promise<ScheduleLeaseRecord> {
    return Promise.reject(new Error("not used"));
  }

  release(): Promise<ScheduleLeaseRecord> {
    return Promise.reject(new Error("not used"));
  }

  markFailed(): Promise<ScheduleLeaseRecord> {
    return Promise.reject(new Error("not used"));
  }

  get(): Promise<undefined> {
    return Promise.resolve(undefined);
  }

  close(): Promise<void> {
    return Promise.resolve();
  }
}

class ProductionTestBrokerTransport implements RuntimeBrokerTransport {
  readonly name = "rabbitmq-payload-publisher";
  connectCount = 0;

  connect(): Promise<void> {
    this.connectCount += 1;
    return Promise.resolve();
  }

  probe() {
    return Promise.resolve({
      status: "ok" as const,
      summary: "RabbitMQ publisher-confirm transport ready"
    });
  }

  assertTopology(_routes: readonly WorkerRoute[]): Promise<void> {
    void _routes;
    return Promise.resolve();
  }

  publish(_command: BrokerPublishCommand): Promise<BrokerPublishReceipt> {
    void _command;
    return Promise.reject(new Error("not used"));
  }

  consume(_stage: WorkerStage, _handler: BrokerDeliveryHandler) {
    void _handler;
    return Promise.resolve({
      stage: _stage,
      cancel: () => Promise.resolve()
    });
  }

  drain(): Promise<void> {
    return Promise.resolve();
  }

  close(): Promise<void> {
    return Promise.resolve();
  }
}

class DeadlineProductionBrokerTransport extends ProductionTestBrokerTransport {
  readonly publishStarted = deferred<undefined>();
  private readonly publishGate = deferred<undefined>();
  private readonly publishCompleted = deferred<undefined>();
  publishCount = 0;

  override async publish(command: BrokerPublishCommand): Promise<BrokerPublishReceipt> {
    this.publishCount += 1;
    this.publishStarted.resolve(undefined);
    await this.publishGate.promise;
    const route = getWorkerRoute(command.envelope.route);

    this.publishCompleted.resolve(undefined);
    return {
      messageId: command.envelope.messageId,
      stage: command.envelope.route,
      exchange: route.exchange,
      routingKey: route.routingKey,
      confirmed: true,
      confirmedAt: new Date().toISOString()
    };
  }

  async finishPublish(): Promise<void> {
    this.publishGate.resolve(undefined);
    await this.publishCompleted.promise;
  }
}

interface Deferred<T> {
  readonly promise: Promise<T>;
  resolve(value: T): void;
}

function deferred<T>(): Deferred<T> {
  let resolveValue: ((value: T) => void) | undefined;
  const promise = new Promise<T>((resolve) => {
    resolveValue = resolve;
  });

  return {
    promise,
    resolve: (value) => {
      resolveValue?.(value);
    }
  };
}

function requestUrl(input: Parameters<typeof fetch>[0] | undefined): string | undefined {
  if (typeof input === "string") {
    return input;
  }

  if (input instanceof URL) {
    return input.toString();
  }

  return input?.url;
}

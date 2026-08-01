import {
  assertWorkerEnvelope,
  validateStagePayload
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  createBufferedRuntimeTelemetrySink,
  type RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  describe,
  expect,
  it
} from "vitest";

import { loadSchedulerConfig } from "../src/config.js";
import type { SchedulerDependencies } from "../src/dependencies.js";
import { SequenceSchedulerIdFactory } from "../src/ids.js";
import { InMemoryScheduleLeaseStore } from "../src/lease-store.js";
import type {
  ScheduleLeaseAcquireCommand,
  ScheduleLeaseRecord,
  ScheduleLeaseStore
} from "../src/lease-store.js";
import { SchedulerPublishError } from "../src/publish-error.js";
import { createSchedulerService } from "../src/service.js";
import type { SchedulerFeedDefinition } from "../src/scheduling.js";
import {
  LocalBrokerTransport,
  ManualSchedulerClock,
  createLocalFeedSource
} from "../src/test-doubles.js";

const uuids = [
  "018f1598-2dd5-7c4f-9f92-8f7a7f8b3701",
  "018f1598-2dd5-7c4f-9f92-8f7a7f8b3702",
  "018f1598-2dd5-7c4f-9f92-8f7a7f8b3703",
  "018f1598-2dd5-7c4f-9f92-8f7a7f8b3704",
  "018f1598-2dd5-7c4f-9f92-8f7a7f8b3705",
  "018f1598-2dd5-7c4f-9f92-8f7a7f8b3706",
  "018f1598-2dd5-7c4f-9f92-8f7a7f8b3707",
  "018f1598-2dd5-7c4f-9f92-8f7a7f8b3708"
] as const;

describe("scheduler publish flow", () => {
  it("publishes one confirmed request for a feed window and does not duplicate the confirmed window", async () => {
    const context = createServiceContext();

    await context.service.start();
    const first = await context.service.runOnce();
    const second = await context.service.runOnce();

    expect(first).toMatchObject({
      dueFeedCount: 1,
      scheduledCount: 1,
      confirmedCount: 1,
      failedCount: 0
    });
    expect(second).toMatchObject({
      dueFeedCount: 1,
      scheduledCount: 0,
      confirmedCount: 0,
      failedCount: 0
    });
    expect(context.broker.published).toHaveLength(1);

    const command = context.broker.published[0];
    if (command === undefined) {
      throw new Error("expected one published command");
    }

    assertWorkerEnvelope(command.envelope);
    expect(validateStagePayload(command.payload).ok).toBe(true);
    expect(command.envelope.idempotencyKey).toBe("scheduler:feed:feed-world:20260723t000500000z");
    expect(command.payload).toMatchObject({
      feedId: "feed-world",
      feedUrl: "https://feeds.example.test/world.xml",
      fetchReason: "scheduled",
      limits: {
        scheduleWindowStart: "2026-07-23T00:05:00.000Z",
        scheduleWindowEnd: "2026-07-23T00:06:00.000Z"
      }
    });

    const lease = await context.dependencies.leaseStore.get(command.envelope.idempotencyKey);
    expect(lease?.status).toBe("confirmed");

    await context.service.stop();
  });

  it("does not let an occupied lease consume the scheduling concurrency allowance", async () => {
    const context = createServiceContext([
      dueFeed("feed-occupied", 10),
      dueFeed("feed-next", 9)
    ], new FirstFeedAlreadyConfirmedLeaseStore(), undefined, {
      NUTSNEWS_SCHEDULER_CONCURRENCY: "1"
    });

    await context.service.start();
    const result = await context.service.runOnce();

    expect(result).toMatchObject({
      dueFeedCount: 2,
      scheduledCount: 1,
      confirmedCount: 1,
      failedCount: 0
    });
    expect(context.broker.published).toHaveLength(1);
    expect(context.broker.published[0]?.payload).toMatchObject({
      feedId: "feed-next"
    });

    await context.service.stop();
  });

  it("keeps a confirmed lease confirmed when confirmation telemetry rejects", async () => {
    let rejectedConfirmationEvents = 0;
    const telemetry: RuntimeTelemetrySink = {
      emit: (event) => {
        if (event.attributes?.event === "scheduler.feed.confirmed") {
          rejectedConfirmationEvents += 1;
          return Promise.reject(new Error("telemetry unavailable"));
        }

        return undefined;
      }
    };
    const context = createServiceContext(undefined, new InMemoryScheduleLeaseStore(), telemetry);

    await context.service.start();
    const first = await context.service.runOnce();
    const command = context.broker.published[0];

    if (command === undefined) {
      throw new Error("expected one published command");
    }

    expect(first).toMatchObject({
      scheduledCount: 1,
      confirmedCount: 1,
      failedCount: 0
    });
    expect(rejectedConfirmationEvents).toBe(1);
    expect(await context.dependencies.leaseStore.get(command.envelope.idempotencyKey)).toMatchObject({
      status: "confirmed",
      attemptCount: 1
    });

    const second = await context.service.runOnce();
    expect(second).toMatchObject({
      scheduledCount: 0,
      confirmedCount: 0,
      failedCount: 0
    });
    expect(context.broker.published).toHaveLength(1);

    await context.service.stop();
  });

  it("marks a failed publish retryable and confirms on a later retry", async () => {
    const context = createServiceContext();

    context.broker.failPublishes = true;
    await context.service.start();
    const failed = await context.service.runOnce();

    expect(failed).toMatchObject({
      scheduledCount: 1,
      confirmedCount: 0,
      failedCount: 1
    });

    context.broker.failPublishes = false;
    const retried = await context.service.runOnce();

    expect(retried).toMatchObject({
      scheduledCount: 1,
      confirmedCount: 1,
      failedCount: 0
    });
    expect(context.broker.published).toHaveLength(1);

    const lease = await context.dependencies.leaseStore.get("scheduler:feed:feed-world:20260723t000500000z");
    expect(lease).toMatchObject({
      status: "confirmed",
      attemptCount: 2
    });

    await context.service.stop();
  });

  it("retains an ambiguous confirm-timeout lease until expiry", async () => {
    const context = createServiceContext();
    const timeout = new Error("publisher confirm timed out");

    timeout.name = "ConfirmTimeoutError";
    context.broker.publishError = timeout;

    await context.service.start();
    const result = await context.service.runOnce();

    expect(result).toMatchObject({
      scheduledCount: 1,
      confirmedCount: 0,
      failedCount: 1
    });
    expect(context.telemetry.events.some((event) => event.attributes?.dependency === "broker" && event.attributes.error === "ConfirmTimeoutError")).toBe(true);
    expect(context.telemetry.events).toContainEqual(expect.objectContaining({
      name: "runtime.dependency.observed",
      level: "error",
      outcome: "failure",
      attributes: expect.objectContaining({
        event: "scheduler.run.completed",
        failedCount: 1
      }) as Record<string, unknown>
    }));
    expect(await context.dependencies.leaseStore.get(
      "scheduler:feed:feed-world:20260723t000500000z"
    )).toMatchObject({
      status: "leased",
      attemptCount: 1
    });

    context.broker.publishError = undefined;
    await expect(context.service.runOnce()).resolves.toMatchObject({
      scheduledCount: 0,
      confirmedCount: 0,
      failedCount: 0
    });

    await context.service.stop();
  });

  it("releases only a publish known not to have reached RabbitMQ", async () => {
    const context = createServiceContext();

    context.broker.publishError = new SchedulerPublishError(
      "connection unavailable before publish",
      "not-published"
    );
    await context.service.start();
    await expect(context.service.runOnce()).resolves.toMatchObject({
      scheduledCount: 1,
      confirmedCount: 0,
      failedCount: 1
    });

    expect(await context.dependencies.leaseStore.get(
      "scheduler:feed:feed-world:20260723t000500000z"
    )).toMatchObject({
      status: "released",
      attemptCount: 1
    });

    await context.service.stop();
  });

  it("never downgrades a confirmed lease after an ambiguous database response", async () => {
    const delegate = new InMemoryScheduleLeaseStore();
    const context = createServiceContext(undefined, new AmbiguousConfirmationLeaseStore(delegate));

    await context.service.start();
    const result = await context.service.runOnce();

    expect(result).toMatchObject({
      scheduledCount: 1,
      confirmedCount: 0,
      failedCount: 1
    });
    expect(context.broker.published).toHaveLength(1);
    expect(await delegate.get("scheduler:feed:feed-world:20260723t000500000z")).toMatchObject({
      status: "confirmed",
      attemptCount: 1
    });

    await expect(context.service.runOnce()).resolves.toMatchObject({
      scheduledCount: 0,
      confirmedCount: 0,
      failedCount: 0
    });
    expect(context.broker.published).toHaveLength(1);

    await context.service.stop();
  });

  it("emits bounded lease-store failure telemetry when the database dependency is unavailable", async () => {
    const context = createServiceContext(undefined, new FailingLeaseStore());

    await context.service.start();
    const result = await context.service.runOnce();

    expect(result).toMatchObject({
      dueFeedCount: 1,
      scheduledCount: 0,
      confirmedCount: 0,
      failedCount: 1
    });
    expect(context.telemetry.events.some((event) =>
      event.attributes?.dependency === "lease-store" &&
      event.attributes.feedId === "feed-world" &&
      event.attributes.windowStart === "2026-07-23T00:05:00.000Z"
    )).toBe(true);

    await context.service.stop();
  });

  it("waits for an in-flight publish during shutdown without wall-clock sleeps", async () => {
    const context = createServiceContext();
    const gate = deferred<undefined>();
    const started = deferred<undefined>();

    context.broker.publishGate = gate.promise;
    context.broker.onPublishStart = () => {
      started.resolve(undefined);
    };

    await context.service.start();
    const run = context.service.runOnce();
    await started.promise;
    const stop = context.service.stop();

    expect(context.service.isDraining).toBe(true);
    expect(context.broker.published).toHaveLength(0);

    gate.resolve(undefined);
    await run;
    await stop;

    expect(context.broker.published).toHaveLength(1);
    expect(context.service.isStarted).toBe(false);
  });

  it("emits observable skip telemetry for disabled, backoff, and not-due feeds", async () => {
    const context = createServiceContext([
      {
        feedId: "feed-disabled",
        feedUrl: "https://feeds.example.test/disabled.xml",
        enabled: false,
        cadenceMs: 60_000,
        priority: 1,
        shardIndex: 0,
        shardCount: 3
      },
      {
        feedId: "feed-backoff",
        feedUrl: "https://feeds.example.test/backoff.xml",
        enabled: true,
        cadenceMs: 60_000,
        priority: 1,
        shardIndex: 1,
        shardCount: 3,
        backoffUntil: "2026-07-23T00:10:00.000Z"
      },
      {
        feedId: "feed-not-due",
        feedUrl: "https://feeds.example.test/not-due.xml",
        enabled: true,
        cadenceMs: 60_000,
        priority: 1,
        shardIndex: 2,
        shardCount: 3,
        lastScheduledAt: "2026-07-23T00:05:00.000Z"
      }
    ]);

    await context.service.start();
    const result = await context.service.runOnce();

    expect(result).toMatchObject({
      dueFeedCount: 0,
      skippedCount: 3,
      confirmedCount: 0
    });
    expect(context.telemetry.events.filter((event) => event.attributes?.event === "scheduler.feed.skipped")).toHaveLength(3);
    expect(context.broker.published).toHaveLength(0);

    await context.service.stop();
  });
});

function createServiceContext(feeds: readonly SchedulerFeedDefinition[] | undefined = [
  {
    feedId: "feed-world",
    feedUrl: "https://feeds.example.test/world.xml",
    enabled: true,
    cadenceMs: 60_000,
    priority: 10,
    shardIndex: 0,
    shardCount: 1,
    limits: {
      timeoutMs: 15_000,
      maxItems: 35
    }
  }
], leaseStore?: ScheduleLeaseStore, telemetrySink?: RuntimeTelemetrySink, configOverrides: NodeJS.ProcessEnv = {}) {
  const config = loadSchedulerConfig({
    NUTSNEWS_SCHEDULER_TELEMETRY_LOGS: "silent",
    NUTSNEWS_SCHEDULER_LEASE_MS: "300000",
    NUTSNEWS_SCHEDULER_CADENCE_MS: "60000",
    ...configOverrides
  });
  const clock = new ManualSchedulerClock("2026-07-23T00:05:42.000Z");
  const broker = new LocalBrokerTransport();
  const resolvedLeaseStore = leaseStore ?? new InMemoryScheduleLeaseStore(() => clock.now());
  const dependencies: SchedulerDependencies = {
    mode: "test",
    clockKind: "manual-test",
    brokerKind: "local-test",
    clock,
    feedSource: createLocalFeedSource({
      feeds
    }),
    leaseStore: resolvedLeaseStore,
    brokerTransport: broker,
    brokerProbe: broker
  };
  const telemetry = createBufferedRuntimeTelemetrySink();
  const service = createSchedulerService({
    config,
    dependencies,
    telemetry: telemetrySink ?? telemetry,
    idFactory: new SequenceSchedulerIdFactory(uuids)
  });

  return {
    broker,
    clock,
    dependencies,
    service,
    telemetry
  };
}

function dueFeed(feedId: string, priority: number): SchedulerFeedDefinition {
  return {
    feedId,
    feedUrl: `https://feeds.example.test/${feedId}.xml`,
    enabled: true,
    cadenceMs: 60_000,
    priority,
    shardIndex: 0,
    shardCount: 2
  };
}

class FirstFeedAlreadyConfirmedLeaseStore extends InMemoryScheduleLeaseStore {
  override acquire(command: ScheduleLeaseAcquireCommand) {
    if (command.feedId !== "feed-occupied") {
      return super.acquire(command);
    }

    return Promise.resolve({
      status: "already_confirmed" as const,
      record: {
        token: "occupied-feed-token",
        feedId: command.feedId,
        idempotencyKey: command.idempotencyKey,
        window: command.window,
        status: "confirmed" as const,
        acquiredAt: command.now.toISOString(),
        leaseExpiresAt: new Date(command.now.getTime() + command.leaseMs).toISOString(),
        attemptCount: 1,
        confirmedAt: command.now.toISOString(),
        publishReceiptMessageId: "already-published-message"
      }
    });
  }
}

class AmbiguousConfirmationLeaseStore implements ScheduleLeaseStore {
  readonly name = "ambiguous-confirmation-test-lease-store";
  readonly adapterKind = "in-memory-test" as const;

  constructor(private readonly delegate: ScheduleLeaseStore) {}

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

  async markConfirmed(token: string, confirmedAt: Date, messageId: string): Promise<ScheduleLeaseRecord> {
    await this.delegate.markConfirmed(token, confirmedAt, messageId);
    throw new Error("database response lost after confirmation commit");
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

class FailingLeaseStore implements ScheduleLeaseStore {
  readonly name = "failing-test-lease-store";
  readonly adapterKind = "in-memory-test" as const;

  probe(): Promise<{ readonly status: "unhealthy"; readonly summary: string }> {
    return Promise.resolve({
      status: "unhealthy",
      summary: "test lease store unavailable"
    });
  }

  acquire(_command: ScheduleLeaseAcquireCommand): Promise<never> {
    void _command;
    const error = new Error("database unavailable");
    error.name = "DatabaseUnavailableError";
    return Promise.reject(error);
  }

  markConfirmed(): Promise<never> {
    return Promise.reject(new Error("not implemented"));
  }

  renew(): Promise<never> {
    return Promise.reject(new Error("not implemented"));
  }

  release(): Promise<never> {
    return Promise.reject(new Error("not implemented"));
  }

  markFailed(): Promise<never> {
    return Promise.reject(new Error("not implemented"));
  }

  get(): Promise<undefined> {
    return Promise.resolve(undefined);
  }

  close(): Promise<void> {
    return Promise.resolve();
  }
}

interface Deferred<T> {
  readonly promise: Promise<T>;
  resolve(value: T): void;
  reject(error: unknown): void;
}

function deferred<T>(): Deferred<T> {
  let resolveValue: ((value: T) => void) | undefined;
  let rejectValue: ((error: unknown) => void) | undefined;
  const promise = new Promise<T>((resolve, reject) => {
    resolveValue = resolve;
    rejectValue = reject;
  });

  return {
    promise,
    resolve: (value) => {
      resolveValue?.(value);
    },
    reject: (error) => {
      rejectValue?.(error);
    }
  };
}

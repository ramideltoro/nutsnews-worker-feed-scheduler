import {
  assertWorkerEnvelope,
  validateStagePayload
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  createBufferedRuntimeTelemetrySink
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

function createServiceContext(feeds: readonly SchedulerFeedDefinition[] = [
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
]) {
  const config = loadSchedulerConfig({
    NUTSNEWS_SCHEDULER_TELEMETRY_LOGS: "silent",
    NUTSNEWS_SCHEDULER_LEASE_MS: "300000",
    NUTSNEWS_SCHEDULER_CADENCE_MS: "60000"
  });
  const clock = new ManualSchedulerClock("2026-07-23T00:05:42.000Z");
  const broker = new LocalBrokerTransport();
  const dependencies: SchedulerDependencies = {
    clock,
    feedSource: createLocalFeedSource({
      feeds
    }),
    leaseStore: new InMemoryScheduleLeaseStore(),
    brokerTransport: broker
  };
  const telemetry = createBufferedRuntimeTelemetrySink();
  const service = createSchedulerService({
    config,
    dependencies,
    telemetry,
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

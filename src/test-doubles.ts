import {
  getWorkerRoute,
  type WorkerMessageEnvelope,
  type WorkerRoute,
  type WorkerStage
} from "@ramideltoro/nutsnews-worker-contracts";
import type {
  BrokerDeliveryHandler,
  BrokerPublishCommand,
  BrokerPublishReceipt,
  RuntimeBrokerTransport,
  RuntimeClock
} from "@ramideltoro/nutsnews-worker-runtime";

import type {
  SchedulerDependencies,
  SchedulerDependencyProbe,
  SchedulerFeedSource
} from "./dependencies.js";
import {
  InMemoryScheduleLeaseStore
} from "./lease-store.js";
import type { SchedulerFeedDefinition } from "./scheduling.js";
import { SchedulerPublishError } from "./publish-error.js";

export class ManualSchedulerClock implements RuntimeClock {
  private current: Date;

  constructor(initial = "2026-07-23T00:00:00.000Z") {
    this.current = new Date(initial);
  }

  now(): Date {
    return new Date(this.current.getTime());
  }

  advance(ms: number): void {
    this.current = new Date(this.current.getTime() + ms);
  }
}

export interface LocalFeedSourceOptions {
  readonly dueFeedCount?: number;
  readonly feeds?: readonly SchedulerFeedDefinition[];
  readonly status?: SchedulerDependencyProbe["status"];
}

export function createLocalFeedSource(options: LocalFeedSourceOptions = {}): SchedulerFeedSource {
  const dueFeedCount = options.dueFeedCount ?? 0;
  const feeds = options.feeds ?? createLocalDueFeeds(dueFeedCount);
  const status = options.status ?? "ok";

  return {
    name: "local-feed-source",
    adapterKind: "local-test",
    probe: () => ({
      status,
      summary: status === "ok" ? "local test feed source ready" : "local test feed source degraded"
    }),
    listActiveFeeds: () => feeds,
    countDueFeeds: () => dueFeedCount
  };
}

export class LocalBrokerTransport implements RuntimeBrokerTransport {
  readonly name: string = "local-broker-transport";
  readonly published: BrokerPublishCommand[] = [];
  readonly assertedRoutes: WorkerRoute[] = [];
  readonly inFlightDeliveryCount = 0;
  failPublishes = false;
  publishError: Error | undefined;
  publishGate: Promise<void> | undefined;
  onPublishStart: (() => void) | undefined;
  private connected = false;
  private closed = false;

  probe(): Promise<SchedulerDependencyProbe> {
    return Promise.resolve({
      status: this.connected && !this.closed ? "ok" : "unhealthy",
      summary: this.connected && !this.closed
        ? "local test broker ready"
        : "local test broker unavailable"
    });
  }

  connect(): Promise<void> {
    this.connected = true;
    this.closed = false;
    return Promise.resolve();
  }

  assertTopology(routes: readonly WorkerRoute[]): Promise<void> {
    this.assertedRoutes.splice(0, this.assertedRoutes.length, ...routes);
    return Promise.resolve();
  }

  publish(command: BrokerPublishCommand): Promise<BrokerPublishReceipt> {
    if (!this.connected || this.closed) {
      throw new Error("Local broker transport is not connected.");
    }

    this.onPublishStart?.();

    return this.publishAfterGate(command);
  }

  private async publishAfterGate(command: BrokerPublishCommand): Promise<BrokerPublishReceipt> {
    await this.publishGate;

    if (this.publishError !== undefined) {
      throw this.publishError;
    }

    if (this.failPublishes) {
      throw new SchedulerPublishError("local publish failure", "rejected");
    }

    this.published.push(command);
    const route = getWorkerRoute(command.envelope.route);

    return {
      messageId: command.envelope.messageId,
      stage: command.envelope.route,
      exchange: route.exchange,
      routingKey: route.routingKey,
      confirmed: true,
      confirmedAt: command.envelope.occurredAt
    };
  }

  consume(stage: WorkerStage, _handler: BrokerDeliveryHandler): Promise<{ readonly stage: WorkerStage; cancel(): Promise<void> }> {
    void _handler;
    return Promise.resolve({
      stage,
      cancel: () => Promise.resolve()
    });
  }

  drain(): Promise<void> {
    return Promise.resolve();
  }

  close(): Promise<void> {
    this.closed = true;
    this.connected = false;
    return Promise.resolve();
  }
}

export function createLocalSchedulerDependencies(options: LocalFeedSourceOptions = {}): SchedulerDependencies {
  const brokerTransport = new LocalBrokerTransport();
  const clock = new ManualSchedulerClock();

  return {
    mode: "test",
    clockKind: "manual-test",
    brokerKind: "local-test",
    clock,
    feedSource: createLocalFeedSource(options),
    leaseStore: new InMemoryScheduleLeaseStore(() => clock.now()),
    brokerTransport,
    brokerProbe: brokerTransport
  };
}

export function createLocalDueFeeds(count: number): readonly SchedulerFeedDefinition[] {
  return Array.from({
    length: count
  }, (_value, index) => {
    const feedNumber = index + 1;

    return {
      feedId: `feed-${String(feedNumber)}`,
      feedUrl: `https://feeds.example.test/feed-${String(feedNumber)}.xml`,
      enabled: true,
      cadenceMs: 60_000,
      priority: count - index,
      shardIndex: index,
      shardCount: Math.max(1, count),
      limits: {
        timeoutMs: 15_000,
        maxItems: 35
      }
    };
  });
}

export function createMinimalFetchEnvelope(overrides: Partial<WorkerMessageEnvelope> = {}): WorkerMessageEnvelope {
  const route = getWorkerRoute("fetch");
  const now = "2026-07-23T00:00:00.000Z";

  return {
    schemaId: route.schemaId,
    schemaVersion: 1,
    route: "fetch",
    messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3620",
    causationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3620",
    correlationId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3620",
    traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    idempotencyKey: "feed:local:fixture",
    aggregate: {
      type: "feed",
      id: "local-fixture-feed",
      version: 1
    },
    occurredAt: now,
    attempt: {
      count: 1,
      max: 4,
      firstAttemptAt: now
    },
    producer: {
      name: "scheduler",
      version: "0.1.0"
    },
    payloadRef: {
      kind: "backend-record",
      uri: "backend://worker-uplift/feed-fetch/local-fixture-feed",
      mediaType: "application/json",
      sizeBytes: 256
    },
    ...overrides
  };
}

import type {
  RuntimeBrokerTransport,
  RuntimeClock
} from "@ramideltoro/nutsnews-worker-runtime";

import type { SchedulerDependencyMode } from "./config.js";
import type { ScheduleLeaseStore } from "./lease-store.js";
import type { SchedulerFeedDefinition } from "./scheduling.js";

export interface SchedulerDependencyProbe {
  readonly status: "ok" | "degraded" | "unhealthy";
  readonly summary: string;
}

export interface SchedulerFeedSource {
  readonly name: string;
  readonly adapterKind: "local-test" | "backend-api";
  probe(): SchedulerDependencyProbe | Promise<SchedulerDependencyProbe>;
  listActiveFeeds(now: Date): readonly SchedulerFeedDefinition[] | Promise<readonly SchedulerFeedDefinition[]>;
  countDueFeeds(now: Date): number | Promise<number>;
}

export interface SchedulerBrokerProbe {
  readonly name: string;
  probe(): SchedulerDependencyProbe | Promise<SchedulerDependencyProbe>;
}

export interface SchedulerDependencies {
  readonly mode: SchedulerDependencyMode;
  readonly clockKind: "manual-test" | "system";
  readonly brokerKind: "local-test" | "rabbitmq";
  readonly clock: RuntimeClock;
  readonly feedSource: SchedulerFeedSource;
  readonly leaseStore: ScheduleLeaseStore;
  readonly brokerTransport: RuntimeBrokerTransport;
  readonly brokerProbe: SchedulerBrokerProbe;
}

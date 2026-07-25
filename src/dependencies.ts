import type {
  RuntimeBrokerTransport,
  RuntimeClock
} from "@ramideltoro/nutsnews-worker-runtime";

export interface SchedulerDependencyProbe {
  readonly status: "ok" | "degraded" | "unhealthy";
  readonly summary: string;
}

export interface SchedulerFeedSource {
  readonly name: string;
  probe(): SchedulerDependencyProbe | Promise<SchedulerDependencyProbe>;
  countDueFeeds(now: Date): number | Promise<number>;
}

export interface SchedulerDependencies {
  readonly clock: RuntimeClock;
  readonly feedSource: SchedulerFeedSource;
  readonly brokerTransport: RuntimeBrokerTransport;
}

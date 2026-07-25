import type { SchedulerFeedDefinition } from "./scheduling.js";

export const SCHEDULER_FIXTURE_NOW = "2026-07-23T00:05:42.000Z" as const;

export const SCHEDULER_FIXTURE_FEEDS = [
  {
    feedId: "feed-world",
    feedUrl: "https://feeds.example.test/world.xml",
    enabled: true,
    cadenceMs: 60_000,
    priority: 10,
    shardIndex: 0,
    shardCount: 4,
    limits: {
      timeoutMs: 15_000,
      maxItems: 35
    }
  },
  {
    feedId: "feed-disabled",
    feedUrl: "https://feeds.example.test/disabled.xml",
    enabled: false,
    disabledReason: "fixture-disabled",
    cadenceMs: 60_000,
    priority: 5,
    shardIndex: 1,
    shardCount: 4
  },
  {
    feedId: "feed-backoff",
    feedUrl: "https://feeds.example.test/backoff.xml",
    enabled: true,
    backoffUntil: "2026-07-23T00:10:00.000Z",
    cadenceMs: 60_000,
    priority: 5,
    shardIndex: 2,
    shardCount: 4
  },
  {
    feedId: "feed-not-due",
    feedUrl: "https://feeds.example.test/not-due.xml",
    enabled: true,
    lastScheduledAt: "2026-07-23T00:05:00.000Z",
    cadenceMs: 60_000,
    priority: 5,
    shardIndex: 3,
    shardCount: 4
  }
] as const satisfies readonly SchedulerFeedDefinition[];

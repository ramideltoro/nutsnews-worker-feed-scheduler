import {
  describe,
  expect,
  it
} from "vitest";

import {
  evaluateFeedSchedule,
  idempotencyKeyFor,
  scheduleWindowFor,
  selectDueFeeds,
  type SchedulerFeedDefinition
} from "../src/scheduling.js";

const baseFeed: SchedulerFeedDefinition = {
  feedId: "feed-world",
  feedUrl: "https://feeds.example.test/world.xml",
  enabled: true,
  cadenceMs: 60_000,
  priority: 10,
  shardIndex: 0,
  shardCount: 1
};

describe("feed scheduling decisions", () => {
  it("calculates a stable window and idempotency key", () => {
    const now = new Date("2026-07-23T00:05:42.123Z");
    const window = scheduleWindowFor(baseFeed, now);

    expect(window).toEqual({
      start: "2026-07-23T00:05:00.000Z",
      end: "2026-07-23T00:06:00.000Z",
      key: "20260723t000500000z"
    });
    expect(idempotencyKeyFor(baseFeed.feedId, window)).toBe("scheduler:feed:feed-world:20260723t000500000z");
  });

  it("skips disabled, backed off, and not-due feeds with observable reasons", () => {
    const now = new Date("2026-07-23T00:05:00.000Z");
    const disabled = evaluateFeedSchedule({
      ...baseFeed,
      enabled: false,
      disabledReason: "source-disabled"
    }, now);
    const backoff = evaluateFeedSchedule({
      ...baseFeed,
      feedId: "feed-backoff",
      backoffUntil: "2026-07-23T00:10:00.000Z"
    }, now);
    const notDue = evaluateFeedSchedule({
      ...baseFeed,
      feedId: "feed-not-due",
      lastScheduledAt: "2026-07-23T00:04:30.000Z"
    }, now);

    expect(disabled).toMatchObject({
      status: "skipped",
      reason: "disabled"
    });
    expect(backoff).toMatchObject({
      status: "skipped",
      reason: "backoff"
    });
    expect(notDue).toMatchObject({
      status: "skipped",
      reason: "not_due"
    });
  });

  it("orders due feeds by priority then feed id", () => {
    const result = selectDueFeeds([
      {
        ...baseFeed,
        feedId: "feed-b",
        priority: 1
      },
      {
        ...baseFeed,
        feedId: "feed-a",
        priority: 10
      },
      {
        ...baseFeed,
        feedId: "feed-c",
        priority: 10
      }
    ], new Date("2026-07-23T00:05:00.000Z"), 10);

    expect(result.due.map((decision) => decision.feed.feedId)).toEqual([
      "feed-a",
      "feed-c",
      "feed-b"
    ]);
  });
});

import {
  describe,
  expect,
  it
} from "vitest";

import { InMemoryScheduleLeaseStore } from "../src/lease-store.js";
import {
  idempotencyKeyFor,
  scheduleWindowFor
} from "../src/scheduling.js";
import { SCHEDULER_FIXTURE_FEEDS } from "../src/fixtures.js";

const feed = SCHEDULER_FIXTURE_FEEDS[0];

describe("schedule lease store", () => {
  it("allows at most one simultaneous lease claim for a feed window", async () => {
    const store = new InMemoryScheduleLeaseStore();
    const now = new Date("2026-07-23T00:05:42.000Z");
    const window = scheduleWindowFor(feed, now);
    const idempotencyKey = idempotencyKeyFor(feed.feedId, window);
    const attempts = await Promise.all([
      store.acquire({
        feedId: feed.feedId,
        idempotencyKey,
        window,
        now,
        leaseMs: 300_000
      }),
      store.acquire({
        feedId: feed.feedId,
        idempotencyKey,
        window,
        now,
        leaseMs: 300_000
      })
    ]);

    expect(attempts.filter((attempt) => attempt.status === "acquired")).toHaveLength(1);
    expect(attempts.filter((attempt) => attempt.status === "lease_active")).toHaveLength(1);
  });

  it("recovers stale claims after the documented lease bound", async () => {
    const store = new InMemoryScheduleLeaseStore();
    const now = new Date("2026-07-23T00:05:42.000Z");
    const recoveredAt = new Date("2026-07-23T00:10:42.001Z");
    const window = scheduleWindowFor(feed, now);
    const idempotencyKey = idempotencyKeyFor(feed.feedId, window);

    await store.acquire({
      feedId: feed.feedId,
      idempotencyKey,
      window,
      now,
      leaseMs: 300_000
    });
    const recovered = await store.acquire({
      feedId: feed.feedId,
      idempotencyKey,
      window,
      now: recoveredAt,
      leaseMs: 300_000
    });

    expect(recovered).toMatchObject({
      status: "acquired",
      record: {
        attemptCount: 2
      }
    });
  });
});

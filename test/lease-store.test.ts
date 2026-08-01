import {
  describe,
  expect,
  it
} from "vitest";

import {
  InMemoryScheduleLeaseStore,
  ScheduleLeaseOwnershipError
} from "../src/lease-store.js";
import {
  idempotencyKeyFor,
  scheduleWindowFor
} from "../src/scheduling.js";
import { SCHEDULER_FIXTURE_FEEDS } from "../src/fixtures.js";

const feed = SCHEDULER_FIXTURE_FEEDS[0];

describe("schedule lease store", () => {
  it("allows at most one simultaneous lease claim for a feed window", async () => {
    const now = new Date("2026-07-23T00:05:42.000Z");
    const store = new InMemoryScheduleLeaseStore(() => now);
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
    let current = new Date("2026-07-23T00:05:42.000Z");
    const store = new InMemoryScheduleLeaseStore(() => current);
    const now = current;
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
    current = recoveredAt;
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

  it("reclaims at the exact expiry boundary with a fresh fenced token", async () => {
    let current = new Date("2026-07-23T00:05:42.000Z");
    const store = new InMemoryScheduleLeaseStore(() => current);
    const now = current;
    const expiresAt = new Date(now.getTime() + 300_000);
    const window = scheduleWindowFor(feed, now);
    const idempotencyKey = idempotencyKeyFor(feed.feedId, window);
    const initial = await store.acquire({
      feedId: feed.feedId,
      idempotencyKey,
      window,
      now,
      leaseMs: 300_000
    });
    current = expiresAt;
    const reclaimed = await store.acquire({
      feedId: feed.feedId,
      idempotencyKey,
      window,
      now: expiresAt,
      leaseMs: 300_000
    });

    expect(reclaimed.status).toBe("acquired");
    expect(reclaimed.record.token).not.toBe(initial.record.token);
    expect(reclaimed.record.attemptCount).toBe(2);
  });

  it("rejects every owner mutation at the exact expiry boundary", async () => {
    let current = new Date("2026-07-23T00:05:42.000Z");
    const store = new InMemoryScheduleLeaseStore(() => current);
    const window = scheduleWindowFor(feed, current);
    const idempotencyKey = idempotencyKeyFor(feed.feedId, window);
    const acquired = await store.acquire({
      feedId: feed.feedId,
      idempotencyKey,
      window,
      now: current,
      leaseMs: 300_000
    });
    current = new Date(current.getTime() + 300_000);
    const staleCallerTime = new Date(current.getTime() - 300_000);

    expect(() => store.renew(acquired.record.token, 300_000)).toThrow(ScheduleLeaseOwnershipError);
    expect(() => store.release(acquired.record.token, staleCallerTime)).toThrow(ScheduleLeaseOwnershipError);
    expect(() => store.markConfirmed(acquired.record.token, staleCallerTime, "receipt-1")).toThrow(ScheduleLeaseOwnershipError);
    expect(() => store.markFailed(acquired.record.token, staleCallerTime, "expired")).toThrow(ScheduleLeaseOwnershipError);
  });

  it("renews only an unexpired owner, caps duration, and preserves confirmed rows", async () => {
    let current = new Date("2026-07-23T00:05:42.000Z");
    const store = new InMemoryScheduleLeaseStore(() => current);
    const window = scheduleWindowFor(feed, current);
    const idempotencyKey = idempotencyKeyFor(feed.feedId, window);
    const acquired = await store.acquire({
      feedId: feed.feedId,
      idempotencyKey,
      window,
      now: current,
      leaseMs: 300_000
    });
    current = new Date(current.getTime() + 299_999);
    const renewed = await store.renew(acquired.record.token, 300_000);

    expect(renewed.leaseExpiresAt).toBe(new Date(current.getTime() + 300_000).toISOString());
    expect(() => store.renew(acquired.record.token, 300_001)).toThrow(/through 300000/u);

    const confirmed = await store.markConfirmed(
      acquired.record.token,
      current,
      "receipt-1"
    );
    expect(confirmed.status).toBe("confirmed");
    expect(() => store.release(acquired.record.token, current)).toThrow(ScheduleLeaseOwnershipError);
    expect(() => store.markFailed(acquired.record.token, current, "late failure")).toThrow(ScheduleLeaseOwnershipError);
  });
});

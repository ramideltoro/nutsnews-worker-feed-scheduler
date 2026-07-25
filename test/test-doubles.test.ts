import { getWorkerRoute } from "@ramideltoro/nutsnews-worker-contracts";
import { createBrokerLifecycle } from "@ramideltoro/nutsnews-worker-runtime";
import {
  describe,
  expect,
  it
} from "vitest";

import {
  LocalBrokerTransport,
  ManualSchedulerClock,
  createLocalFeedSource,
  createMinimalFetchEnvelope
} from "../src/test-doubles.js";

describe("local test doubles", () => {
  it("provides a controllable clock", () => {
    const clock = new ManualSchedulerClock("2026-07-23T00:00:00.000Z");

    clock.advance(1_000);

    expect(clock.now().toISOString()).toBe("2026-07-23T00:00:01.000Z");
  });

  it("provides a feed source probe and due-feed count", async () => {
    const feedSource = createLocalFeedSource({
      dueFeedCount: 3
    });

    expect(await feedSource.probe()).toMatchObject({
      status: "ok"
    });
    expect(await feedSource.countDueFeeds(new Date())).toBe(3);
  });

  it("provides a broker transport that confirms fetch-route publishes", async () => {
    const transport = new LocalBrokerTransport();
    const lifecycle = createBrokerLifecycle({
      transport,
      routes: [
        getWorkerRoute("fetch")
      ]
    });
    const envelope = createMinimalFetchEnvelope();

    await lifecycle.start();
    const receipt = await lifecycle.publish({
      envelope,
      payload: {
        fixture: true
      }
    });

    expect(receipt.confirmed).toBe(true);
    expect(receipt.routingKey).toBe(getWorkerRoute("fetch").routingKey);
    expect(transport.published).toHaveLength(1);

    await lifecycle.stop();
  });
});

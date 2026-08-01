import {
  describe,
  expect,
  it
} from "vitest";

import { loadSchedulerConfig } from "../src/config.js";
import {
  createSchedulerApplication,
  createSchedulerApplicationDependencies
} from "../src/index.js";
import {
  LocalBrokerTransport
} from "../src/test-doubles.js";

describe("scheduler application lifecycle", () => {
  it("uses the system clock for the real application dependency set", () => {
    const before = Date.now();
    const observed = createSchedulerApplicationDependencies().clock.now().getTime();
    const after = Date.now();

    expect(observed).toBeGreaterThanOrEqual(before);
    expect(observed).toBeLessThanOrEqual(after);
  });

  it("binds health endpoints without awaiting a hung first scheduling iteration", async () => {
    const config = loadSchedulerConfig({
      NUTSNEWS_SCHEDULER_HTTP_HOST: "127.0.0.1",
      NUTSNEWS_SCHEDULER_HTTP_PORT: "0",
      NUTSNEWS_SCHEDULER_TELEMETRY_LOGS: "silent"
    });
    const dependencies = createSchedulerApplicationDependencies();
    let releaseFeedRead: (() => void) | undefined;
    let feedReads = 0;
    const blockedFeedRead = new Promise<void>((resolve) => {
      releaseFeedRead = resolve;
    });
    const application = createSchedulerApplication(config, {
      ...dependencies,
      feedSource: {
        ...dependencies.feedSource,
        listActiveFeeds: async () => {
          feedReads += 1;
          await blockedFeedRead;
          return [];
        }
      }
    });

    await application.start();
    expect(feedReads).toBe(1);

    releaseFeedRead?.();
    await new Promise<void>((resolve) => {
      setImmediate(resolve);
    });
    await application.stop();
  });

  it("binds diagnostics before a blocked broker startup settles", async () => {
    const config = loadSchedulerConfig({
      NUTSNEWS_SCHEDULER_HTTP_HOST: "127.0.0.1",
      NUTSNEWS_SCHEDULER_HTTP_PORT: "0",
      NUTSNEWS_SCHEDULER_TELEMETRY_LOGS: "silent"
    });
    const dependencies = createSchedulerApplicationDependencies();
    const connectGate = deferredSignal();
    const brokerTransport = new GatedBrokerTransport(connectGate.promise);
    const application = createSchedulerApplication(config, {
      ...dependencies,
      brokerTransport
    });
    const startup = application.start();
    const liveUrl = await waitForUrl(application, "/live");

    expect((await fetch(liveUrl)).status).toBe(200);
    expect((await fetch(application.url("/startupz"))).status).toBe(503);
    expect((await fetch(application.url("/readyz"))).status).toBe(503);
    const metricsResponse = await fetch(application.url("/metrics"));
    const metrics = await metricsResponse.text();
    expect(metricsResponse.status).toBe(200);
    expect(metrics).toMatch(/nutsnews_worker_health_probe\{(?=[^\n}]*probe="liveness")(?=[^\n}]*outcome="ok")[^\n}]*\} 1/u);
    expect(metrics).toMatch(/nutsnews_worker_health_probe\{(?=[^\n}]*probe="startup")(?=[^\n}]*outcome="unhealthy")[^\n}]*\} 1/u);
    expect(metrics).toMatch(/nutsnews_worker_health_probe\{(?=[^\n}]*probe="readiness")(?=[^\n}]*outcome="unhealthy")[^\n}]*\} 1/u);

    connectGate.resolve();
    await startup;
    expect((await fetch(application.url("/startupz"))).status).toBe(200);

    await application.stop();
  });

  it("preserves the startup error when failed-start cleanup also rejects", async () => {
    const config = loadSchedulerConfig({
      NUTSNEWS_SCHEDULER_HTTP_HOST: "127.0.0.1",
      NUTSNEWS_SCHEDULER_HTTP_PORT: "0",
      NUTSNEWS_SCHEDULER_TELEMETRY_LOGS: "silent",
      NUTSNEWS_SCHEDULER_METRICS_ENABLED: "false"
    });
    const dependencies = createSchedulerApplicationDependencies();
    const startupError = new Error("broker startup failed");
    const cleanupError = new Error("broker cleanup failed");
    const application = createSchedulerApplication(config, {
      ...dependencies,
      brokerTransport: new FailingStartupBroker(startupError, cleanupError)
    });

    await expect(application.start()).rejects.toBe(startupError);
  });
});

class GatedBrokerTransport extends LocalBrokerTransport {
  constructor(private readonly connectGate: Promise<void>) {
    super();
  }

  override async connect(): Promise<void> {
    await this.connectGate;
    await super.connect();
  }
}

class FailingStartupBroker extends LocalBrokerTransport {
  override readonly name = "failing-startup-broker";

  constructor(
    private readonly startupError: Error,
    private readonly cleanupError: Error
  ) {
    super();
  }

  override connect(): Promise<void> {
    return Promise.reject(this.startupError);
  }

  override close(): Promise<void> {
    return Promise.reject(this.cleanupError);
  }
}

async function waitForUrl(
  application: ReturnType<typeof createSchedulerApplication>,
  path: string
): Promise<string> {
  for (let attempt = 0; attempt < 50; attempt += 1) {
    try {
      return application.url(path);
    } catch {
      await new Promise<void>((resolve) => {
        setImmediate(resolve);
      });
    }
  }

  throw new Error("scheduler HTTP server did not bind in time");
}

function deferredSignal() {
  let resolve!: () => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<void>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });

  return {
    promise,
    resolve,
    reject
  };
}

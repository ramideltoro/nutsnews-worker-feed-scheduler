import {
  createBufferedRuntimeTelemetrySink,
  createPrometheusRuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  describe,
  expect,
  it
} from "vitest";

import { loadSchedulerConfig } from "../src/config.js";
import { createSchedulerService } from "../src/service.js";
import { createLocalSchedulerDependencies } from "../src/test-doubles.js";

describe("createSchedulerService", () => {
  it("starts, becomes ready, records a dry scheduler check, and drains cleanly", async () => {
    const config = loadSchedulerConfig({
      NUTSNEWS_SCHEDULER_HTTP_PORT: "0",
      NUTSNEWS_SCHEDULER_TELEMETRY_LOGS: "silent"
    });
    const dependencies = createLocalSchedulerDependencies({
      dueFeedCount: 2
    });
    const telemetry = createBufferedRuntimeTelemetrySink();
    const metrics = createPrometheusRuntimeTelemetrySink({
      identity: {
        service: config.serviceName,
        version: config.serviceVersion,
        environment: config.environment,
        host: config.host
      }
    });
    const service = createSchedulerService({
      config,
      dependencies,
      telemetry,
      metrics
    });

    await service.start();

    expect(service.isStarted).toBe(true);
    expect((await service.health.liveness()).status).toBe("ok");
    expect((await service.health.startup()).status).toBe("ok");
    expect((await service.health.readiness()).status).toBe("ok");

    await expect(service.runOnce()).resolves.toMatchObject({
      dueFeedCount: 2,
      shadowMode: true
    });
    expect(metrics.collect()).toContain("nutsnews_worker_dependency_duration_ms");

    await service.stop();

    expect(service.isStarted).toBe(false);
    expect(service.broker.state).toBe("closed");
    expect(telemetry.events.some((event) => event.name === "runtime.broker.state_changed")).toBe(true);
  });

  it("reports readiness unhealthy when the local feed source is unhealthy", async () => {
    const config = loadSchedulerConfig({
      NUTSNEWS_SCHEDULER_TELEMETRY_LOGS: "silent"
    });
    const service = createSchedulerService({
      config,
      dependencies: createLocalSchedulerDependencies({
        status: "unhealthy"
      })
    });

    await service.start();

    expect((await service.health.readiness()).status).toBe("unhealthy");

    await service.stop();
  });
});

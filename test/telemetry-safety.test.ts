import {
  createBufferedRuntimeTelemetrySink,
  createPrometheusRuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  describe,
  expect,
  it
} from "vitest";

import {
  combineBestEffortTelemetrySinks,
  schedulerMetricsTelemetrySink
} from "../src/telemetry-safety.js";

describe("scheduler telemetry safety", () => {
  it("isolates each configured sink so a rejection does not starve later sinks", async () => {
    let observed = 0;
    const telemetry = combineBestEffortTelemetrySinks(
      {
        emit: () => Promise.reject(new Error("log unavailable"))
      },
      {
        emit: () => {
          observed += 1;
        }
      }
    );

    await expect(telemetry?.emit({
      name: "runtime.broker.state_changed",
      level: "info",
      at: "2026-07-23T00:00:00.000Z",
      outcome: "success"
    })).resolves.toBeUndefined();
    expect(observed).toBe(1);
  });

  it("keeps health evaluation logs while suppressing the shared runtime health metric family", async () => {
    const logs = createBufferedRuntimeTelemetrySink();
    const runtimeMetrics = createPrometheusRuntimeTelemetrySink({
      identity: {
        service: "nutsnews-worker-feed-scheduler",
        version: "0.1.0",
        environment: "test",
        host: "scheduler-test"
      }
    });
    const telemetry = combineBestEffortTelemetrySinks(
      logs,
      schedulerMetricsTelemetrySink(runtimeMetrics)
    );

    await telemetry?.emit({
      name: "runtime.health.evaluated",
      level: "info",
      at: "2026-07-23T00:00:00.000Z",
      outcome: "ok",
      attributes: {
        probe: "readiness",
        status: "ok"
      }
    });

    expect(logs.events).toHaveLength(1);
    expect(logs.events[0]?.name).toBe("runtime.health.evaluated");
    expect(runtimeMetrics.collect()).not.toMatch(/^# TYPE nutsnews_worker_health gauge$/mu);
    expect(runtimeMetrics.collect()).not.toMatch(/^nutsnews_worker_health\{/mu);
  });

  it("does not turn duration-less startup observations into zero-latency samples", async () => {
    let observations = 0;
    const metrics = schedulerMetricsTelemetrySink({
      emit: () => {
        observations += 1;
      }
    });

    await metrics?.emit({
      name: "runtime.dependency.observed",
      level: "info",
      at: "2026-07-23T00:00:00.000Z",
      outcome: "success",
      attributes: {
        dependency: "scheduler-shell"
      }
    });
    await metrics?.emit({
      name: "runtime.dependency.observed",
      level: "info",
      at: "2026-07-23T00:00:00.010Z",
      durationMs: 10,
      outcome: "success",
      attributes: {
        dependency: "feed-source"
      }
    });

    expect(observations).toBe(1);
  });
});

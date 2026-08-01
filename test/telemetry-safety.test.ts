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
  bestEffortSchedulerMetricsSink,
  combineBestEffortTelemetrySinks
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

  it("forwards Runtime 1 health events to the canonical probe metric family", async () => {
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
      bestEffortSchedulerMetricsSink(runtimeMetrics)
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
    expect(runtimeMetrics.collect()).toMatch(
      /nutsnews_worker_health_probe\{(?=[^\n}]*probe="readiness")(?=[^\n}]*outcome="ok")[^\n}]*\} 1/u
    );
    expect(runtimeMetrics.collect()).not.toMatch(/^# TYPE nutsnews_worker_health gauge$/mu);
    expect(runtimeMetrics.collect()).not.toMatch(/^nutsnews_worker_health\{/mu);
  });

  it("relies on Runtime 1 to ignore duration-less dependency observations", async () => {
    const runtimeMetrics = createPrometheusRuntimeTelemetrySink({
      identity: {
        service: "nutsnews-worker-feed-scheduler",
        version: "0.1.0",
        environment: "test",
        host: "scheduler-test"
      },
      cardinality: {
        dependencies: [
          "scheduler-shell",
          "feed-source"
        ]
      }
    });
    const metrics = bestEffortSchedulerMetricsSink(runtimeMetrics);

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

    expect(runtimeMetrics.collect()).toMatch(
      /nutsnews_worker_dependency_duration_seconds_count\{[^\n]+dependency="feed-source"[^\n]*\} 1/u
    );
    expect(runtimeMetrics.collect()).not.toContain('dependency="scheduler-shell"');
  });
});

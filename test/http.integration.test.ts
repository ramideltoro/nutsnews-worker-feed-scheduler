import { createPrometheusRuntimeTelemetrySink } from "@ramideltoro/nutsnews-worker-runtime";
import {
  afterEach,
  describe,
  expect,
  it
} from "vitest";

import { loadSchedulerConfig } from "../src/config.js";
import {
  createSchedulerHttpServer,
  type SchedulerHttpServer
} from "../src/http.js";
import {
  createSchedulerFailClosedReconciler
} from "../src/reconciliation.js";
import { createSchedulerService } from "../src/service.js";
import {
  ManualSchedulerClock,
  createLocalSchedulerDependencies
} from "../src/test-doubles.js";

let activeServer: SchedulerHttpServer | undefined;

afterEach(async () => {
  if (activeServer !== undefined) {
    await activeServer.close();
    activeServer = undefined;
  }
});

describe("scheduler HTTP endpoints", () => {
  it("serves liveness, readiness, startup, metrics, and value-free config schema", async () => {
    const config = loadSchedulerConfig({
      NUTSNEWS_SCHEDULER_HTTP_HOST: "127.0.0.1",
      NUTSNEWS_SCHEDULER_HTTP_PORT: "0",
      NUTSNEWS_SCHEDULER_TELEMETRY_LOGS: "silent"
    });
    const metrics = createPrometheusRuntimeTelemetrySink({
      identity: {
        service: config.serviceName,
        version: config.serviceVersion,
        environment: config.environment,
        host: config.host,
        revision: config.buildRevision,
        deployment: "shadow",
        adapter: "in_memory"
      }
    });
    const service = createSchedulerService({
      config,
      dependencies: createLocalSchedulerDependencies(),
      metrics
    });
    activeServer = createSchedulerHttpServer({
      config,
      service,
      metrics
    });

    await service.start();
    await activeServer.listen();
    await service.runOnce();

    await expectJsonStatus(activeServer.url("/live"), 200, "ok");
    await expectJsonStatus(activeServer.url("/startup"), 200, "ok");
    await expectJsonStatus(activeServer.url("/ready"), 200, "ok");

    const metricsResponse = await fetch(activeServer.url("/metrics"));
    expect(metricsResponse.status).toBe(200);
    const metricsBody = await metricsResponse.text();
    expect(metricsBody).toContain("nutsnews_worker_build_info");
    expect(metricsBody).toContain('version="0.1.0",revision="unknown"');
    expect(metricsBody).toContain('deployment="shadow",adapter="in_memory"');
    expect(metricsBody).toContain("nutsnews_worker_expected_active");
    expect(metricsBody).toContain("nutsnews_worker_last_success_timestamp_seconds");
    expect(metricsBody.match(/^# TYPE nutsnews_worker_health_probe gauge$/gmu)).toHaveLength(1);
    expect(metricsBody.match(/^# TYPE nutsnews_worker_health_check gauge$/gmu)).toHaveLength(1);
    expect(metricsBody).toMatch(
      /nutsnews_worker_health_probe\{(?=[^\n}]*probe="readiness")(?=[^\n}]*outcome="ok")[^\n}]*\} 1/u
    );
    expect(metricsBody).toMatch(
      /nutsnews_worker_last_success_timestamp_seconds\{[^\n}]*\} [1-9][0-9]*/u
    );
    expect(metricsBody).not.toMatch(/^# TYPE nutsnews_worker_health gauge$/mu);
    expect(metricsBody).not.toMatch(/^nutsnews_worker_health\{/mu);

    const schemaResponse = await fetch(activeServer.url("/config-schema"));
    expect(schemaResponse.status).toBe(200);
    const schema = await schemaResponse.json() as { readonly variables: readonly { readonly name: string; readonly sensitive: boolean }[] };
    expect(schema.variables.some((variable) => variable.name === "NUTSNEWS_SCHEDULER_RABBITMQ_URL" && variable.sensitive)).toBe(true);
    expect(JSON.stringify(schema)).not.toContain("postgres://");

    await service.stop();
  });

  it("protects the reconciliation endpoint with bearer auth", async () => {
    const config = loadSchedulerConfig({
      NUTSNEWS_SCHEDULER_HTTP_HOST: "127.0.0.1",
      NUTSNEWS_SCHEDULER_HTTP_PORT: "0",
      NUTSNEWS_SCHEDULER_TELEMETRY_LOGS: "silent"
    });
    const service = createSchedulerService({
      config,
      dependencies: createLocalSchedulerDependencies()
    });
    activeServer = createSchedulerHttpServer({
      config,
      service,
      reconciler: createSchedulerFailClosedReconciler(new ManualSchedulerClock()),
      reconciliationToken: "test-token"
    });

    await service.start();
    await activeServer.listen();

    const unauthorized = await fetch(activeServer.url("/reconcile/outbox"), {
      method: "POST",
      body: JSON.stringify({
        mode: "dry-run"
      })
    });
    expect(unauthorized.status).toBe(401);

    const authorized = await fetch(activeServer.url("/reconcile/outbox"), {
      method: "POST",
      headers: {
        authorization: "Bearer test-token"
      },
      body: JSON.stringify({
        mode: "dry-run"
      })
    });
    expect(authorized.status).toBe(200);
    await expect(authorized.json()).resolves.toMatchObject({
      status: "dry_run",
      writesPerformed: false,
      productionVisibilityEnabled: false
    });

    await service.stop();
  });

  it("returns an empty metrics body when metrics are disabled", async () => {
    const config = loadSchedulerConfig({
      NUTSNEWS_SCHEDULER_HTTP_HOST: "127.0.0.1",
      NUTSNEWS_SCHEDULER_HTTP_PORT: "0",
      NUTSNEWS_SCHEDULER_TELEMETRY_LOGS: "silent",
      NUTSNEWS_SCHEDULER_METRICS_ENABLED: "false"
    });
    const service = createSchedulerService({
      config,
      dependencies: createLocalSchedulerDependencies()
    });
    activeServer = createSchedulerHttpServer({
      config,
      service
    });

    await service.start();
    await activeServer.listen();

    const response = await fetch(activeServer.url("/metrics"));
    expect(response.status).toBe(200);
    expect(await response.text()).toBe("");

    await service.stop();
  });
});

async function expectJsonStatus(url: string, statusCode: number, status: string): Promise<void> {
  const response = await fetch(url);
  const body = await response.json() as { readonly status: string };

  expect(response.status).toBe(statusCode);
  expect(body.status).toBe(status);
}

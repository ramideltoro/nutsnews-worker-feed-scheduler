import {
  describe,
  expect,
  it
} from "vitest";

import {
  SchedulerConfigError,
  loadSchedulerConfig
} from "../src/config.js";

describe("loadSchedulerConfig", () => {
  it("loads local test defaults without secret values", () => {
    const config = loadSchedulerConfig({});

    expect(config.serviceName).toBe("nutsnews-worker-feed-scheduler");
    expect(config.serviceVersion).toBe("0.1.0");
    expect(config.dependencyMode).toBe("test");
    expect(config.dependencies).toEqual({
      databaseConfigured: false,
      backendApiUrlConfigured: false,
      backendApiTokenConfigured: false,
      rabbitmqConfigured: false
    });
    expect(config.shadowMode).toBe(true);
  });

  it("requires dependency presence in production mode", () => {
    expect(() => loadSchedulerConfig({
      NUTSNEWS_SCHEDULER_DEPENDENCY_MODE: "production"
    })).toThrow(SchedulerConfigError);
  });

  it("accepts production mode when every required dependency is present", () => {
    const config = loadSchedulerConfig({
      NUTSNEWS_SCHEDULER_DEPENDENCY_MODE: "production",
      NUTSNEWS_SCHEDULER_DATABASE_URL: "postgres://configured",
      NUTSNEWS_SCHEDULER_BACKEND_API_URL: "https://backend.example.test",
      NUTSNEWS_SCHEDULER_BACKEND_API_TOKEN: "configured",
      NUTSNEWS_SCHEDULER_RABBITMQ_URL: "amqps://configured",
      NUTSNEWS_SCHEDULER_CADENCE_MS: "60000",
      NUTSNEWS_SCHEDULER_LEASE_MS: "300000"
    });

    expect(config.dependencyMode).toBe("production");
    expect(config.dependencies.rabbitmqConfigured).toBe(true);
  });

  it("keeps shadow mode enabled before backend-owned cutover", () => {
    expect(() => loadSchedulerConfig({
      NUTSNEWS_SCHEDULER_SHADOW_MODE: "false"
    })).toThrow(/SHADOW_MODE/u);
  });

  it("rejects local test dependencies in the production environment", () => {
    expect(() => loadSchedulerConfig({
      NUTSNEWS_ENVIRONMENT: "production",
      NUTSNEWS_SCHEDULER_DEPENDENCY_MODE: "test"
    })).toThrow(/DEPENDENCY_MODE must be production/u);
  });
});

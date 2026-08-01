import {
  describe,
  expect,
  it
} from "vitest";

import {
  SCHEDULER_CONFIG_SCHEMA,
  SchedulerConfigError,
  loadSchedulerConfig
} from "../src/config.js";

describe("loadSchedulerConfig", () => {
  it("loads local test defaults without secret values", () => {
    const config = loadSchedulerConfig({});

    expect(config.serviceName).toBe("nutsnews-worker-feed-scheduler");
    expect(config.serviceVersion).toBe("0.1.0");
    expect(config.buildRevision).toBe("unknown");
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
      NUTSNEWS_SCHEDULER_BUILD_REVISION: "0123456789abcdef0123456789abcdef01234567",
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

  it("reserves at least 60 seconds for bounded production publication", () => {
    const production = {
      NUTSNEWS_SCHEDULER_DEPENDENCY_MODE: "production",
      NUTSNEWS_SCHEDULER_BUILD_REVISION: "0123456789abcdef0123456789abcdef01234567",
      NUTSNEWS_SCHEDULER_DATABASE_URL: "postgres://configured",
      NUTSNEWS_SCHEDULER_BACKEND_API_URL: "https://backend.example.test",
      NUTSNEWS_SCHEDULER_BACKEND_API_TOKEN: "configured",
      NUTSNEWS_SCHEDULER_RABBITMQ_URL: "amqps://configured"
    } as const;

    expect(loadSchedulerConfig({
      ...production,
      NUTSNEWS_SCHEDULER_CADENCE_MS: "60000",
      NUTSNEWS_SCHEDULER_LEASE_MS: "60000"
    }).leaseMs).toBe(60_000);

    expect(() => loadSchedulerConfig({
      ...production,
      NUTSNEWS_SCHEDULER_CADENCE_MS: "30000",
      NUTSNEWS_SCHEDULER_LEASE_MS: "30000"
    })).toThrow(/LEASE_MS must be at least 60000 in production/u);
  });

  it("accepts a lease equal to cadence and rejects values above the 300-second contract", () => {
    expect(loadSchedulerConfig({
      NUTSNEWS_SCHEDULER_CADENCE_MS: "300000",
      NUTSNEWS_SCHEDULER_LEASE_MS: "300000"
    }).leaseMs).toBe(300_000);

    expect(() => loadSchedulerConfig({
      NUTSNEWS_SCHEDULER_LEASE_MS: "300001"
    })).toThrow(/LEASE_MS must be an integer between 1000 and 300000/u);

    expect(() => loadSchedulerConfig({
      NUTSNEWS_SCHEDULER_LEASE_MS: "900000"
    })).toThrow(/LEASE_MS must be an integer between 1000 and 300000/u);
  });

  it("rejects an unknown build revision in production dependency mode", () => {
    expect(() => loadSchedulerConfig({
      NUTSNEWS_SCHEDULER_DEPENDENCY_MODE: "production",
      NUTSNEWS_SCHEDULER_BUILD_REVISION: "UNKNOWN",
      NUTSNEWS_SCHEDULER_DATABASE_URL: "postgres://configured",
      NUTSNEWS_SCHEDULER_BACKEND_API_URL: "https://backend.example.test",
      NUTSNEWS_SCHEDULER_BACKEND_API_TOKEN: "configured",
      NUTSNEWS_SCHEDULER_RABBITMQ_URL: "amqps://configured"
    })).toThrow(/BUILD_REVISION.*immutable build/u);

    expect(SCHEDULER_CONFIG_SCHEMA).toContainEqual(expect.objectContaining({
      name: "NUTSNEWS_SCHEDULER_BUILD_REVISION",
      requiredInProduction: true,
      sensitive: false
    }));
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

  it("bounds the build revision exported in metrics", () => {
    expect(() => loadSchedulerConfig({
      NUTSNEWS_SCHEDULER_BUILD_REVISION: "revision with spaces"
    })).toThrow(/BUILD_REVISION/u);

    expect(loadSchedulerConfig({
      NUTSNEWS_SCHEDULER_BUILD_REVISION: "abc123-deploy.4"
    }).buildRevision).toBe("abc123-deploy.4");
  });
});

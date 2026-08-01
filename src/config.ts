import os from "node:os";

import { SCHEDULE_LEASE_MAX_MS } from "./lease-store.js";

export const SCHEDULER_SERVICE_NAME = "nutsnews-worker-feed-scheduler" as const;
export const SCHEDULER_SERVICE_VERSION = "0.1.0" as const;
export const SCHEDULER_PRODUCTION_MIN_LEASE_MS = 60_000;

export type SchedulerDependencyMode = "test" | "production";
export type SchedulerTelemetryLogMode = "stdout" | "silent";

export interface SchedulerConfigVariable {
  readonly name: string;
  readonly description: string;
  readonly requiredInProduction: boolean;
  readonly sensitive: boolean;
  readonly defaultValue?: string;
}

export const SCHEDULER_CONFIG_SCHEMA = [
  variable("NUTSNEWS_ENVIRONMENT", "Runtime environment label for logs and metrics.", false, false, "local"),
  variable("NUTSNEWS_SCHEDULER_BUILD_REVISION", "Immutable source revision exposed as bounded build identity.", true, false, "unknown"),
  variable("NUTSNEWS_SCHEDULER_HTTP_HOST", "Health and metrics bind host.", false, false, "0.0.0.0"),
  variable("NUTSNEWS_SCHEDULER_HTTP_PORT", "Health and metrics bind port.", false, false, "8080"),
  variable("NUTSNEWS_SCHEDULER_DEPENDENCY_MODE", "Use test dependencies locally or require production dependency presence.", false, false, "test"),
  variable("NUTSNEWS_SCHEDULER_DATABASE_URL", "Backend shadow database connection string.", true, true),
  variable("NUTSNEWS_SCHEDULER_BACKEND_API_URL", "Backend API base URL for feed-source access.", true, false),
  variable("NUTSNEWS_SCHEDULER_BACKEND_API_TOKEN", "Backend API token for feed-source access.", true, true),
  variable("NUTSNEWS_SCHEDULER_RABBITMQ_URL", "Private RabbitMQ connection string.", true, true),
  variable("NUTSNEWS_SCHEDULER_CADENCE_MS", "Scheduler loop cadence in milliseconds.", false, false, "60000"),
  variable("NUTSNEWS_SCHEDULER_LEASE_MS", "Due-feed lease duration in milliseconds.", false, false, "300000"),
  variable("NUTSNEWS_SCHEDULER_CONCURRENCY", "Maximum concurrent due-feed scheduling decisions.", false, false, "4"),
  variable("NUTSNEWS_SCHEDULER_SHUTDOWN_TIMEOUT_MS", "Graceful shutdown drain timeout in milliseconds.", false, false, "30000"),
  variable("NUTSNEWS_SCHEDULER_SHADOW_MODE", "Keep scheduler publication isolated from legacy ingestion.", false, false, "true"),
  variable("NUTSNEWS_SCHEDULER_TELEMETRY_LOGS", "Structured runtime log sink mode.", false, false, "stdout"),
  variable("NUTSNEWS_SCHEDULER_METRICS_ENABLED", "Expose bounded Prometheus metrics.", false, false, "true")
] as const satisfies readonly SchedulerConfigVariable[];

export interface SchedulerConfig {
  readonly serviceName: typeof SCHEDULER_SERVICE_NAME;
  readonly serviceVersion: typeof SCHEDULER_SERVICE_VERSION;
  readonly environment: string;
  readonly host: string;
  readonly buildRevision: string;
  readonly http: {
    readonly host: string;
    readonly port: number;
  };
  readonly dependencyMode: SchedulerDependencyMode;
  readonly dependencies: {
    readonly databaseConfigured: boolean;
    readonly backendApiUrlConfigured: boolean;
    readonly backendApiTokenConfigured: boolean;
    readonly rabbitmqConfigured: boolean;
  };
  readonly cadenceMs: number;
  readonly leaseMs: number;
  readonly concurrency: number;
  readonly shutdownTimeoutMs: number;
  readonly shadowMode: boolean;
  readonly telemetryLogs: SchedulerTelemetryLogMode;
  readonly metricsEnabled: boolean;
}

export class SchedulerConfigError extends Error {
  readonly issues: readonly string[];

  constructor(issues: readonly string[]) {
    super(`Invalid scheduler configuration: ${issues.join("; ")}`);
    this.name = "SchedulerConfigError";
    this.issues = issues;
  }
}

export function loadSchedulerConfig(env: NodeJS.ProcessEnv = process.env): SchedulerConfig {
  const issues: string[] = [];
  const dependencyMode = parseDependencyMode(env.NUTSNEWS_SCHEDULER_DEPENDENCY_MODE, issues);
  const dependencies = {
    databaseConfigured: hasValue(env.NUTSNEWS_SCHEDULER_DATABASE_URL),
    backendApiUrlConfigured: hasValue(env.NUTSNEWS_SCHEDULER_BACKEND_API_URL),
    backendApiTokenConfigured: hasValue(env.NUTSNEWS_SCHEDULER_BACKEND_API_TOKEN),
    rabbitmqConfigured: hasValue(env.NUTSNEWS_SCHEDULER_RABBITMQ_URL)
  };

  if (dependencyMode === "production") {
    requireConfigured("NUTSNEWS_SCHEDULER_DATABASE_URL", dependencies.databaseConfigured, issues);
    requireConfigured("NUTSNEWS_SCHEDULER_BACKEND_API_URL", dependencies.backendApiUrlConfigured, issues);
    requireConfigured("NUTSNEWS_SCHEDULER_BACKEND_API_TOKEN", dependencies.backendApiTokenConfigured, issues);
    requireConfigured("NUTSNEWS_SCHEDULER_RABBITMQ_URL", dependencies.rabbitmqConfigured, issues);
  }

  const config: SchedulerConfig = {
    serviceName: SCHEDULER_SERVICE_NAME,
    serviceVersion: SCHEDULER_SERVICE_VERSION,
    environment: nonEmpty(env.NUTSNEWS_ENVIRONMENT, "local"),
    host: nonEmpty(env.HOSTNAME, os.hostname()),
    buildRevision: parseBuildRevision(env.NUTSNEWS_SCHEDULER_BUILD_REVISION, issues),
    http: {
      host: nonEmpty(env.NUTSNEWS_SCHEDULER_HTTP_HOST, "0.0.0.0"),
      port: parseInteger(env.NUTSNEWS_SCHEDULER_HTTP_PORT, "NUTSNEWS_SCHEDULER_HTTP_PORT", 8080, 0, 65_535, issues)
    },
    dependencyMode,
    dependencies,
    cadenceMs: parseInteger(env.NUTSNEWS_SCHEDULER_CADENCE_MS, "NUTSNEWS_SCHEDULER_CADENCE_MS", 60_000, 1_000, 86_400_000, issues),
    leaseMs: parseInteger(
      env.NUTSNEWS_SCHEDULER_LEASE_MS,
      "NUTSNEWS_SCHEDULER_LEASE_MS",
      SCHEDULE_LEASE_MAX_MS,
      1_000,
      SCHEDULE_LEASE_MAX_MS,
      issues
    ),
    concurrency: parseInteger(env.NUTSNEWS_SCHEDULER_CONCURRENCY, "NUTSNEWS_SCHEDULER_CONCURRENCY", 4, 1, 128, issues),
    shutdownTimeoutMs: parseInteger(env.NUTSNEWS_SCHEDULER_SHUTDOWN_TIMEOUT_MS, "NUTSNEWS_SCHEDULER_SHUTDOWN_TIMEOUT_MS", 30_000, 1_000, 600_000, issues),
    shadowMode: parseBoolean(env.NUTSNEWS_SCHEDULER_SHADOW_MODE, "NUTSNEWS_SCHEDULER_SHADOW_MODE", true, issues),
    telemetryLogs: parseTelemetryLogMode(env.NUTSNEWS_SCHEDULER_TELEMETRY_LOGS, issues),
    metricsEnabled: parseBoolean(env.NUTSNEWS_SCHEDULER_METRICS_ENABLED, "NUTSNEWS_SCHEDULER_METRICS_ENABLED", true, issues)
  };

  if (config.dependencyMode === "production" && config.buildRevision.toLowerCase() === "unknown") {
    issues.push("NUTSNEWS_SCHEDULER_BUILD_REVISION must identify an immutable build when NUTSNEWS_SCHEDULER_DEPENDENCY_MODE=production.");
  }

  if (config.leaseMs < config.cadenceMs) {
    issues.push("NUTSNEWS_SCHEDULER_LEASE_MS must be greater than or equal to NUTSNEWS_SCHEDULER_CADENCE_MS.");
  }

  if (config.dependencyMode === "production" && config.leaseMs < SCHEDULER_PRODUCTION_MIN_LEASE_MS) {
    issues.push(`NUTSNEWS_SCHEDULER_LEASE_MS must be at least ${String(SCHEDULER_PRODUCTION_MIN_LEASE_MS)} in production so bounded publication and terminal fencing finish before expiry.`);
  }

  if (!config.shadowMode) {
    issues.push("NUTSNEWS_SCHEDULER_SHADOW_MODE must remain true until backend-owned deployment enables cutover.");
  }

  if (config.environment.toLowerCase() === "production" && config.dependencyMode !== "production") {
    issues.push("NUTSNEWS_SCHEDULER_DEPENDENCY_MODE must be production when NUTSNEWS_ENVIRONMENT=production.");
  }

  if (issues.length > 0) {
    throw new SchedulerConfigError(issues);
  }

  return config;
}

function variable(
  name: string,
  description: string,
  requiredInProduction: boolean,
  sensitive: boolean,
  defaultValue?: string
): SchedulerConfigVariable {
  return {
    name,
    description,
    requiredInProduction,
    sensitive,
    ...(defaultValue === undefined ? {} : {
      defaultValue
    })
  };
}

function nonEmpty(value: string | undefined, fallback: string): string {
  if (value === undefined) {
    return fallback;
  }

  const trimmed = value.trim();

  return trimmed.length > 0 ? trimmed : fallback;
}

function hasValue(value: string | undefined): boolean {
  return value !== undefined && value.trim().length > 0;
}

function parseBuildRevision(value: string | undefined, issues: string[]): string {
  const revision = nonEmpty(value, "unknown");

  if (revision.length > 128 || !/^[A-Za-z0-9._/@:+-]+$/u.test(revision)) {
    issues.push("NUTSNEWS_SCHEDULER_BUILD_REVISION must be at most 128 URL-safe identity characters.");
    return "unknown";
  }

  return revision;
}

function parseDependencyMode(value: string | undefined, issues: string[]): SchedulerDependencyMode {
  const normalized = nonEmpty(value, "test");

  if (normalized === "test" || normalized === "production") {
    return normalized;
  }

  issues.push("NUTSNEWS_SCHEDULER_DEPENDENCY_MODE must be test or production.");
  return "test";
}

function parseTelemetryLogMode(value: string | undefined, issues: string[]): SchedulerTelemetryLogMode {
  const normalized = nonEmpty(value, "stdout");

  if (normalized === "stdout" || normalized === "silent") {
    return normalized;
  }

  issues.push("NUTSNEWS_SCHEDULER_TELEMETRY_LOGS must be stdout or silent.");
  return "stdout";
}

function parseBoolean(
  value: string | undefined,
  key: string,
  fallback: boolean,
  issues: string[]
): boolean {
  if (!hasValue(value)) {
    return fallback;
  }

  if (value === undefined) {
    return fallback;
  }

  const normalized = value.trim().toLowerCase();

  if (normalized === "true" || normalized === "1") {
    return true;
  }

  if (normalized === "false" || normalized === "0") {
    return false;
  }

  issues.push(`${key} must be true or false.`);
  return fallback;
}

function parseInteger(
  value: string | undefined,
  key: string,
  fallback: number,
  min: number,
  max: number,
  issues: string[]
): number {
  if (!hasValue(value)) {
    return fallback;
  }

  const parsed = Number(value);

  if (!Number.isInteger(parsed) || parsed < min || parsed > max) {
    issues.push(`${key} must be an integer between ${String(min)} and ${String(max)}.`);
    return fallback;
  }

  return parsed;
}

function requireConfigured(key: string, configured: boolean, issues: string[]): void {
  if (!configured) {
    issues.push(`${key} is required when NUTSNEWS_SCHEDULER_DEPENDENCY_MODE=production.`);
  }
}

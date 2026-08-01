import {
  createHash,
  randomUUID
} from "node:crypto";

import {
  SYSTEM_RUNTIME_CLOCK
} from "@ramideltoro/nutsnews-worker-runtime";
import {
  Pool,
  type PoolClient,
  type QueryResult,
  type QueryResultRow
} from "pg";

import type { SchedulerConfig } from "./config.js";
import type {
  SchedulerDependencies,
  SchedulerDependencyProbe,
  SchedulerFeedSource
} from "./dependencies.js";
import type {
  ScheduleLeaseAcquireCommand,
  ScheduleLeaseAcquireResult,
  ScheduleLeaseRecord,
  ScheduleLeaseStatus,
  ScheduleLeaseStore
} from "./lease-store.js";
import {
  SCHEDULE_LEASE_MAX_MS,
  ScheduleLeaseOwnershipError,
  assertScheduleLeaseDuration
} from "./lease-store.js";
import type { SchedulerFeedDefinition } from "./scheduling.js";
import { createLocalSchedulerDependencies } from "./test-doubles.js";
import { SchedulerRabbitMqPublisherTransport } from "./rabbitmq-publisher.js";

const BACKEND_FEED_PAGE_SIZE = 500;
const BACKEND_FEED_MAX_ROWS = 5_000;
const DEPENDENCY_TIMEOUT_MS = 5_000;
const POSTGRES_SCHEMA = "worker_uplift_scheduler";
const POSTGRES_LEASE_TABLE = `${POSTGRES_SCHEMA}.feed_leases`;

export interface ProductionSchedulerEnvironment {
  readonly NUTSNEWS_SCHEDULER_DATABASE_URL?: string;
  readonly NUTSNEWS_SCHEDULER_BACKEND_API_URL?: string;
  readonly NUTSNEWS_SCHEDULER_BACKEND_API_TOKEN?: string;
  readonly NUTSNEWS_SCHEDULER_RABBITMQ_URL?: string;
}

export interface BackendApiFeedSourceOptions {
  readonly baseUrl: string;
  readonly token: string;
  readonly cadenceMs: number;
  readonly request?: typeof fetch;
  readonly timeoutMs?: number;
}

export function createSchedulerDependencies(
  config: SchedulerConfig,
  env: ProductionSchedulerEnvironment = process.env
): SchedulerDependencies {
  if (config.dependencyMode === "test") {
    return createLocalSchedulerDependencies();
  }

  return createProductionSchedulerDependencies(config, env);
}

export function createProductionSchedulerDependencies(
  config: SchedulerConfig,
  env: ProductionSchedulerEnvironment = process.env
): SchedulerDependencies {
  if (config.dependencyMode !== "production") {
    throw new Error("Production scheduler dependencies require production dependency mode.");
  }

  const databaseUrl = requiredEnvironmentValue(
    env.NUTSNEWS_SCHEDULER_DATABASE_URL,
    "NUTSNEWS_SCHEDULER_DATABASE_URL"
  );
  const backendApiUrl = requiredEnvironmentValue(
    env.NUTSNEWS_SCHEDULER_BACKEND_API_URL,
    "NUTSNEWS_SCHEDULER_BACKEND_API_URL"
  );
  const backendApiToken = requiredEnvironmentValue(
    env.NUTSNEWS_SCHEDULER_BACKEND_API_TOKEN,
    "NUTSNEWS_SCHEDULER_BACKEND_API_TOKEN"
  );
  const rabbitMqUrl = requiredEnvironmentValue(
    env.NUTSNEWS_SCHEDULER_RABBITMQ_URL,
    "NUTSNEWS_SCHEDULER_RABBITMQ_URL"
  );

  const brokerTransport = new SchedulerRabbitMqPublisherTransport({
    url: rabbitMqUrl,
    confirmTimeoutMs: DEPENDENCY_TIMEOUT_MS,
    drainTimeoutMs: config.shutdownTimeoutMs
  });

  return {
    mode: "production",
    clockKind: "system",
    brokerKind: "rabbitmq",
    clock: SYSTEM_RUNTIME_CLOCK,
    feedSource: new BackendApiFeedSource({
      baseUrl: backendApiUrl,
      token: backendApiToken,
      cadenceMs: config.cadenceMs
    }),
    leaseStore: new PostgresScheduleLeaseStore(databaseUrl),
    brokerTransport,
    brokerProbe: brokerTransport
  };
}

export class BackendApiFeedSource implements SchedulerFeedSource {
  readonly name = "backend-api-feed-source";
  readonly adapterKind = "backend-api" as const;
  private readonly endpoint: URL;
  private readonly token: string;
  private readonly cadenceMs: number;
  private readonly request: typeof fetch;
  private readonly timeoutMs: number;

  constructor(options: BackendApiFeedSourceOptions) {
    this.endpoint = new URL(
      "/api/worker/db/load-feeds-for-shard",
      normalizeBaseUrl(options.baseUrl)
    );
    this.token = options.token;
    this.cadenceMs = options.cadenceMs;
    this.request = options.request ?? fetch;
    this.timeoutMs = options.timeoutMs ?? DEPENDENCY_TIMEOUT_MS;
  }

  async probe(): Promise<SchedulerDependencyProbe> {
    try {
      await this.loadPage(1, 0);
      return {
        status: "ok",
        summary: "backend API feed source ready"
      };
    } catch (error: unknown) {
      return {
        status: "unhealthy",
        summary: `backend API feed source unavailable (${errorClass(error)})`
      };
    }
  }

  async listActiveFeeds(_now: Date): Promise<readonly SchedulerFeedDefinition[]> {
    void _now;
    const rows: BackendFeedRow[] = [];

    for (let offset = 0; offset < BACKEND_FEED_MAX_ROWS; offset += BACKEND_FEED_PAGE_SIZE) {
      const page = await this.loadPage(BACKEND_FEED_PAGE_SIZE, offset);
      rows.push(...page);

      if (page.length < BACKEND_FEED_PAGE_SIZE) {
        return rows.map((row, index) => mapBackendFeed(row, index, rows.length, this.cadenceMs));
      }
    }

    throw new SchedulerDependencyError("Backend feed inventory exceeded its bounded row limit.");
  }

  async countDueFeeds(now: Date): Promise<number> {
    const feeds = await this.listActiveFeeds(now);
    return feeds.length;
  }

  private async loadPage(limit: number, offset: number): Promise<BackendFeedRow[]> {
    let response: Response;

    try {
      response = await this.request(this.endpoint, {
        method: "POST",
        headers: {
          accept: "application/json",
          authorization: `Bearer ${this.token}`,
          "content-type": "application/json",
          "x-nutsnews-db-client": "worker-uplift-scheduler"
        },
        body: JSON.stringify({
          providerMode: "backend_postgres_primary",
          feedsPerShard: limit,
          offset
        }),
        signal: AbortSignal.timeout(this.timeoutMs)
      });
    } catch (error: unknown) {
      throw new SchedulerDependencyError(
        `Backend feed-source request failed (${errorClass(error)}).`
      );
    }

    if (!response.ok) {
      throw new SchedulerDependencyError(
        `Backend feed-source request returned HTTP ${String(response.status)}.`
      );
    }

    let payload: unknown;

    try {
      payload = await response.json();
    } catch {
      throw new SchedulerDependencyError("Backend feed-source response was not JSON.");
    }

    if (!Array.isArray(payload) || !payload.every(isBackendFeedRow)) {
      throw new SchedulerDependencyError("Backend feed-source response did not match the feed contract.");
    }

    return payload;
  }
}

export class PostgresScheduleLeaseStore implements ScheduleLeaseStore {
  readonly name = "postgres-schedule-lease-store";
  readonly adapterKind = "postgres" as const;
  private readonly pool: Pool;

  constructor(connectionString: string, pool?: Pool) {
    this.pool = pool ?? new Pool({
      connectionString,
      max: 4,
      connectionTimeoutMillis: DEPENDENCY_TIMEOUT_MS,
      query_timeout: DEPENDENCY_TIMEOUT_MS,
      statement_timeout: DEPENDENCY_TIMEOUT_MS,
      idleTimeoutMillis: 30_000,
      application_name: "nutsnews-worker-feed-scheduler"
    });
  }

  async probe(): Promise<SchedulerDependencyProbe> {
    try {
      await this.pool.query(`SELECT 1 FROM ${POSTGRES_LEASE_TABLE} LIMIT 1`);
      return {
        status: "ok",
        summary: "PostgreSQL schedule lease store ready"
      };
    } catch (error: unknown) {
      return {
        status: "unhealthy",
        summary: `PostgreSQL schedule lease store unavailable (${errorClass(error)})`
      };
    }
  }

  async acquire(command: ScheduleLeaseAcquireCommand): Promise<ScheduleLeaseAcquireResult> {
    assertScheduleLeaseDuration(command.leaseMs);
    const client = await this.pool.connect();

    try {
      await client.query("BEGIN");
      await client.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [
        command.feedId
      ]);
      await releaseExpiredFeedLease(client, command.feedId);
      const active = await activeLeaseForFeed(client, command.feedId);

      if (active !== undefined) {
        if (active.lease_unexpired === true) {
          await client.query("COMMIT");
          return {
            status: "lease_active",
            record: rowToRecord(active)
          };
        }

        await releaseExpiredFeedLease(client, command.feedId);
      }

      const existing = await latestLeaseForIdempotencyKey(
        client,
        command.feedId,
        command.idempotencyKey
      );

      if (existing !== undefined) {
        const record = rowToRecord(existing);

        if (record.status === "confirmed") {
          await client.query("COMMIT");
          return {
            status: "already_confirmed",
            record
          };
        }

      }

      const leaseVersion = await nextFeedLeaseVersion(client, command.feedId);
      const token = randomUUID();
      const inserted = await client.query<DbFeedLeaseRow>(
        `INSERT INTO ${POSTGRES_LEASE_TABLE} (
          feed_url,
          lease_token,
          holder_stage_execution_id,
          lease_version,
          acquired_at,
          expires_at,
          diagnostic_metadata
        ) VALUES (
          $1,$2,$3,$4,
          statement_timestamp(),
          statement_timestamp() + (LEAST($5::integer, $6::integer) * INTERVAL '1 millisecond'),
          $7::jsonb
        )
        RETURNING *, statement_timestamp() AS ownership_checked_at`,
        [
          command.feedId,
          token,
          command.idempotencyKey,
          leaseVersion,
          command.leaseMs,
          SCHEDULE_LEASE_MAX_MS,
          JSON.stringify({
            status: "leased",
            windowStart: command.window.start,
            windowEnd: command.window.end,
            windowKey: command.window.key,
            attemptCount: existing === undefined ? 1 : rowToRecord(existing).attemptCount + 1
          })
        ]
      );
      await client.query("COMMIT");

      return {
        status: "acquired",
        record: rowToRecord(oneRow(inserted))
      };
    } catch (error: unknown) {
      await rollbackQuietly(client);
      throw error;
    } finally {
      client.release();
    }
  }

  async renew(token: string, leaseMs: number): Promise<ScheduleLeaseRecord> {
    assertScheduleLeaseDuration(leaseMs);
    const result = await this.pool.query<DbFeedLeaseRow>(
      `WITH lease_clock AS (
         SELECT statement_timestamp() AS checked_at
       )
       UPDATE ${POSTGRES_LEASE_TABLE} AS lease
       SET expires_at = lease_clock.checked_at
         + (LEAST($2::integer, $3::integer) * INTERVAL '1 millisecond')
       FROM lease_clock
       WHERE lease.lease_token = $1
         AND lease.released_at IS NULL
         AND lease.diagnostic_metadata->>'status' = 'leased'
         AND lease.expires_at > lease_clock.checked_at
       RETURNING lease.*, lease_clock.checked_at AS ownership_checked_at`,
      [
        token,
        leaseMs,
        SCHEDULE_LEASE_MAX_MS
      ]
    );

    return rowToRecord(oneOwnedLeaseRow(result, "renew"));
  }

  async release(token: string, _releasedAt: Date): Promise<ScheduleLeaseRecord> {
    void _releasedAt;
    const result = await this.pool.query<DbFeedLeaseRow>(
      `UPDATE ${POSTGRES_LEASE_TABLE}
       SET released_at = statement_timestamp(),
           diagnostic_metadata = diagnostic_metadata || '{"status":"released"}'::jsonb
       WHERE lease_token = $1
         AND released_at IS NULL
         AND diagnostic_metadata->>'status' = 'leased'
         AND expires_at > statement_timestamp()
       RETURNING *`,
      [
        token
      ]
    );

    return rowToRecord(oneOwnedLeaseRow(result, "release"));
  }

  async markConfirmed(
    token: string,
    _confirmedAt: Date,
    publishReceiptMessageId: string
  ): Promise<ScheduleLeaseRecord> {
    const result = await this.pool.query<DbFeedLeaseRow>(
      `UPDATE ${POSTGRES_LEASE_TABLE}
       SET released_at = statement_timestamp(),
           diagnostic_metadata = diagnostic_metadata || $2::jsonb
       WHERE lease_token = $1
         AND released_at IS NULL
         AND diagnostic_metadata->>'status' = 'leased'
         AND expires_at > statement_timestamp()
       RETURNING *`,
      [
        token,
        JSON.stringify({
          status: "confirmed",
          publishReceiptMessageId
        })
      ]
    );

    return rowToRecord(oneOwnedLeaseRow(result, "confirm"));
  }

  async markFailed(
    token: string,
    _failedAt: Date,
    failureReason: string
  ): Promise<ScheduleLeaseRecord> {
    const result = await this.pool.query<DbFeedLeaseRow>(
      `UPDATE ${POSTGRES_LEASE_TABLE}
       SET released_at = statement_timestamp(),
           diagnostic_metadata = diagnostic_metadata || $2::jsonb
       WHERE lease_token = $1
         AND released_at IS NULL
         AND diagnostic_metadata->>'status' = 'leased'
         AND expires_at > statement_timestamp()
       RETURNING *`,
      [
        token,
        JSON.stringify({
          status: "failed",
          failureReason: failureReason.slice(0, 128)
        })
      ]
    );

    return rowToRecord(oneOwnedLeaseRow(result, "fail"));
  }

  async get(idempotencyKey: string): Promise<ScheduleLeaseRecord | undefined> {
    const result = await this.pool.query<DbFeedLeaseRow>(
      `SELECT *
       FROM ${POSTGRES_LEASE_TABLE}
       WHERE holder_stage_execution_id = $1
       ORDER BY lease_version DESC
       LIMIT 1`,
      [
        idempotencyKey
      ]
    );
    const row = result.rows[0];
    return row === undefined ? undefined : rowToRecord(row);
  }

  async close(): Promise<void> {
    await this.pool.end();
  }
}

export class SchedulerDependencyError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SchedulerDependencyError";
  }
}

interface BackendFeedRow {
  readonly source: string | null;
  readonly url: string;
  readonly is_positive_source: boolean;
}

interface DbFeedLeaseRow {
  readonly feed_url: string;
  readonly lease_token: string;
  readonly holder_stage_execution_id: string;
  readonly lease_version: number | string;
  readonly acquired_at: Date;
  readonly expires_at: Date;
  readonly released_at: Date | null;
  readonly diagnostic_metadata: Readonly<Record<string, unknown>>;
  readonly lease_unexpired?: boolean;
  readonly ownership_checked_at?: Date;
}

function requiredEnvironmentValue(value: string | undefined, name: string): string {
  const normalized = value?.trim();

  if (normalized === undefined || normalized.length === 0) {
    throw new SchedulerDependencyError(`${name} is required for production scheduler dependencies.`);
  }

  return normalized;
}

function normalizeBaseUrl(value: string): URL {
  let parsed: URL;

  try {
    parsed = new URL(value);
  } catch {
    throw new SchedulerDependencyError("Scheduler backend API base URL is invalid.");
  }

  if (parsed.protocol !== "https:" && !isLoopbackHttp(parsed)) {
    throw new SchedulerDependencyError("Scheduler backend API requires HTTPS or loopback HTTP.");
  }

  return parsed;
}

function isLoopbackHttp(value: URL): boolean {
  return value.protocol === "http:" && ["127.0.0.1", "::1", "localhost"].includes(value.hostname);
}

function isBackendFeedRow(value: unknown): value is BackendFeedRow {
  if (!isRecord(value)) {
    return false;
  }

  return typeof value.url === "string"
    && value.url.length > 0
    && typeof value.is_positive_source === "boolean"
    && (value.source === null || typeof value.source === "string");
}

function mapBackendFeed(
  row: BackendFeedRow,
  index: number,
  feedCount: number,
  cadenceMs: number
): SchedulerFeedDefinition {
  return {
    feedId: `feed-${createHash("sha256").update(row.url).digest("hex").slice(0, 32)}`,
    feedUrl: row.url,
    enabled: true,
    cadenceMs,
    priority: row.is_positive_source ? 2 : 1,
    shardIndex: index,
    shardCount: Math.max(1, feedCount),
    limits: {
      timeoutMs: 15_000,
      maxItems: 35
    }
  };
}

async function releaseExpiredFeedLease(
  client: PoolClient,
  feedId: string
): Promise<void> {
  await client.query(
    `UPDATE ${POSTGRES_LEASE_TABLE}
     SET released_at = statement_timestamp(),
         diagnostic_metadata = diagnostic_metadata || '{"status":"expired"}'::jsonb
     WHERE feed_url = $1
       AND released_at IS NULL
       AND expires_at <= statement_timestamp()`,
    [
      feedId
    ]
  );
}

async function latestLeaseForIdempotencyKey(
  client: PoolClient,
  feedId: string,
  idempotencyKey: string
): Promise<DbFeedLeaseRow | undefined> {
  const result = await client.query<DbFeedLeaseRow>(
    `SELECT *,
       released_at IS NULL AND expires_at > statement_timestamp() AS lease_unexpired
     FROM ${POSTGRES_LEASE_TABLE}
     WHERE feed_url = $1
       AND holder_stage_execution_id = $2
     ORDER BY lease_version DESC
     LIMIT 1
     FOR UPDATE`,
    [
      feedId,
      idempotencyKey
    ]
  );
  return result.rows[0];
}

async function activeLeaseForFeed(
  client: PoolClient,
  feedId: string
): Promise<DbFeedLeaseRow | undefined> {
  const result = await client.query<DbFeedLeaseRow>(
    `SELECT *,
       expires_at > statement_timestamp() AS lease_unexpired
     FROM ${POSTGRES_LEASE_TABLE}
     WHERE feed_url = $1
       AND released_at IS NULL
     ORDER BY lease_version DESC
     LIMIT 1
     FOR UPDATE`,
    [
      feedId
    ]
  );

  return result.rows[0];
}

async function nextFeedLeaseVersion(client: PoolClient, feedId: string): Promise<number> {
  const result = await client.query<{ readonly next_version: number | string }>(
    `SELECT COALESCE(MAX(lease_version), 0) + 1 AS next_version
     FROM ${POSTGRES_LEASE_TABLE}
     WHERE feed_url = $1`,
    [
      feedId
    ]
  );
  const value = Number(oneRow(result).next_version);

  if (!Number.isSafeInteger(value) || value < 1) {
    throw new SchedulerDependencyError("PostgreSQL returned an invalid schedule lease version.");
  }

  return value;
}

function rowToRecord(row: DbFeedLeaseRow): ScheduleLeaseRecord {
  const metadata = row.diagnostic_metadata;
  const status = scheduleLeaseStatus(metadata.status);
  const windowStart = stringMetadata(metadata.windowStart) ?? row.acquired_at.toISOString();
  const windowEnd = stringMetadata(metadata.windowEnd) ?? row.expires_at.toISOString();
  const windowKey = stringMetadata(metadata.windowKey)
    ?? row.holder_stage_execution_id.split(":").at(-1)
    ?? "unknown";
  const confirmedAt = stringMetadata(metadata.confirmedAt);
  const failedAt = stringMetadata(metadata.failedAt);
  const releasedAt = stringMetadata(metadata.releasedAt);
  const failureReason = stringMetadata(metadata.failureReason);
  const publishReceiptMessageId = stringMetadata(metadata.publishReceiptMessageId);

  return {
    token: row.lease_token,
    feedId: row.feed_url,
    idempotencyKey: row.holder_stage_execution_id,
    window: {
      start: windowStart,
      end: windowEnd,
      key: windowKey
    },
    status,
    acquiredAt: row.acquired_at.toISOString(),
    leaseExpiresAt: row.expires_at.toISOString(),
    ...(row.ownership_checked_at === undefined ? {} : {
      ownershipCheckedAt: row.ownership_checked_at.toISOString()
    }),
    attemptCount: integerMetadata(metadata.attemptCount, Number(row.lease_version)),
    ...(status !== "confirmed" ? {} : {
      confirmedAt: confirmedAt ?? row.released_at?.toISOString() ?? row.acquired_at.toISOString()
    }),
    ...(status !== "failed" ? {} : {
      failedAt: failedAt ?? row.released_at?.toISOString() ?? row.acquired_at.toISOString()
    }),
    ...(status !== "released" && status !== "expired" ? {} : {
      releasedAt: releasedAt ?? row.released_at?.toISOString() ?? row.expires_at.toISOString()
    }),
    ...(failureReason === undefined ? {} : {
      failureReason
    }),
    ...(publishReceiptMessageId === undefined ? {} : {
      publishReceiptMessageId
    })
  };
}

function scheduleLeaseStatus(value: unknown): ScheduleLeaseStatus {
  return value === "confirmed"
    || value === "failed"
    || value === "released"
    || value === "expired"
    ? value
    : "leased";
}

function stringMetadata(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

function integerMetadata(value: unknown, fallback: number): number {
  return Number.isSafeInteger(value) && Number(value) > 0 ? Number(value) : fallback;
}

function oneRow<Row extends QueryResultRow>(result: QueryResult<Row>): Row {
  const row = result.rows[0];

  if (row === undefined) {
    throw new SchedulerDependencyError("PostgreSQL schedule lease operation returned no row.");
  }

  return row;
}

function oneOwnedLeaseRow(
  result: QueryResult<DbFeedLeaseRow>,
  operation: string
): DbFeedLeaseRow {
  const row = result.rows[0];

  if (row === undefined) {
    throw new ScheduleLeaseOwnershipError(
      `Schedule lease ${operation} rejected because ownership is absent, terminal, or expired.`
    );
  }

  return row;
}

async function rollbackQuietly(client: PoolClient): Promise<void> {
  try {
    await client.query("ROLLBACK");
  } catch {
    // Preserve the original failure while the pooled connection is discarded.
  }
}

function errorClass(error: unknown): string {
  return error instanceof Error && error.name.length > 0 ? error.name : "UnknownError";
}

function isRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

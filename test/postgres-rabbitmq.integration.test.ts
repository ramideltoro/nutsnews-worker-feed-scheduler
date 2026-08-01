import * as amqp from "amqplib";
import type {
  ChannelModel,
  ConfirmChannel
} from "amqplib";
import { Pool } from "pg";
import type { QueryResult } from "pg";
import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it
} from "vitest";
import {
  getWorkerRoute,
  type WorkerRoute,
  type WorkerStage
} from "@ramideltoro/nutsnews-worker-contracts";
import type {
  BrokerDeliveryHandler,
  BrokerPublishCommand,
  BrokerPublishReceipt,
  RuntimeBrokerTransport
} from "@ramideltoro/nutsnews-worker-runtime";

import { loadSchedulerConfig } from "../src/config.js";
import type { SchedulerDependencies } from "../src/dependencies.js";
import { SequenceSchedulerIdFactory } from "../src/ids.js";
import type {
  ScheduleLeaseAcquireCommand,
  ScheduleLeaseAcquireResult,
  ScheduleLeaseRecord,
  ScheduleLeaseStore
} from "../src/lease-store.js";
import { ScheduleLeaseOwnershipError } from "../src/lease-store.js";
import { PostgresScheduleLeaseStore } from "../src/production-dependencies.js";
import { SchedulerRabbitMqPublisherTransport } from "../src/rabbitmq-publisher.js";
import { createSchedulerService } from "../src/service.js";
import type { SchedulerFeedDefinition } from "../src/scheduling.js";
import {
  ManualSchedulerClock,
  createLocalFeedSource
} from "../src/test-doubles.js";

const postgresUrl = process.env.SCHEDULER_INTEGRATION_POSTGRES_URL;
const rabbitmqUrl = process.env.SCHEDULER_INTEGRATION_RABBITMQ_URL;
const describeWithServices = postgresUrl !== undefined && rabbitmqUrl !== undefined ? describe : describe.skip;

describeWithServices("PostgreSQL and RabbitMQ scheduler integration", () => {
  let pool: Pool;
  const transports: RabbitMqTestTransport[] = [];

  beforeEach(async () => {
    pool = new Pool({
      connectionString: postgresUrl,
      max: 4
    });
    await pool.query("CREATE SCHEMA IF NOT EXISTS worker_uplift_scheduler");
    await pool.query(`
      CREATE TABLE IF NOT EXISTS worker_uplift_scheduler.feed_leases (
        id bigserial PRIMARY KEY,
        feed_url text NOT NULL,
        lease_token text NOT NULL,
        holder_stage_execution_id text NOT NULL,
        lease_version integer NOT NULL CHECK (lease_version > 0),
        acquired_at timestamptz NOT NULL,
        expires_at timestamptz NOT NULL,
        released_at timestamptz,
        diagnostic_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
        redact_after timestamptz NOT NULL DEFAULT (now() + interval '30 days'),
        UNIQUE (feed_url, lease_version),
        UNIQUE (lease_token)
      )
    `);
    await pool.query(`
      CREATE UNIQUE INDEX IF NOT EXISTS worker_uplift_scheduler_active_feed_lease_uidx
      ON worker_uplift_scheduler.feed_leases (feed_url)
      WHERE released_at IS NULL
    `);
    await pool.query("TRUNCATE worker_uplift_scheduler.feed_leases");
  });

  afterEach(async () => {
    for (const transport of transports.splice(0)) {
      await transport.close();
    }

    await pool.end();
  });

  it("lets concurrent replicas claim and publish at most one request for a schedule window", async () => {
    const feed: SchedulerFeedDefinition = {
      feedId: "feed-world",
      feedUrl: "https://feeds.example.test/world.xml",
      enabled: true,
      cadenceMs: 60_000,
      priority: 10,
      shardIndex: 0,
      shardCount: 1
    };
    const firstTransport = new RabbitMqTestTransport(rabbitmqUrl);
    const secondTransport = new RabbitMqTestTransport(rabbitmqUrl);
    transports.push(firstTransport, secondTransport);
    const first = createReplica(firstTransport, [
      "018f1598-2dd5-7c4f-9f92-8f7a7f8b3801",
      "018f1598-2dd5-7c4f-9f92-8f7a7f8b3802",
      "018f1598-2dd5-7c4f-9f92-8f7a7f8b3803",
      "018f1598-2dd5-7c4f-9f92-8f7a7f8b3804"
    ], feed);
    const second = createReplica(secondTransport, [
      "018f1598-2dd5-7c4f-9f92-8f7a7f8b3811",
      "018f1598-2dd5-7c4f-9f92-8f7a7f8b3812",
      "018f1598-2dd5-7c4f-9f92-8f7a7f8b3813",
      "018f1598-2dd5-7c4f-9f92-8f7a7f8b3814"
    ], feed);

    await Promise.all([
      first.service.start(),
      second.service.start()
    ]);
    const results = await Promise.all([
      first.service.runOnce(),
      second.service.runOnce()
    ]);

    expect(results.reduce((sum, result) => sum + result.confirmedCount, 0)).toBe(1);
    expect(results.reduce((sum, result) => sum + result.failedCount, 0)).toBe(0);
    expect(await firstTransport.queueMessageCount()).toBe(1);

    await Promise.all([
      first.service.stop(),
      second.service.stop()
    ]);
  });

  it("uses PostgreSQL time, fresh tokens, and exact-boundary ownership fencing", async () => {
    const store = new PostgresScheduleLeaseStore(postgresUrl ?? "");
    const command: ScheduleLeaseAcquireCommand = {
      feedId: "feed-server-clock",
      idempotencyKey: "scheduler:feed:feed-server-clock:20260723t000500000z",
      window: {
        start: "2026-07-23T00:05:00.000Z",
        end: "2026-07-23T00:10:00.000Z",
        key: "20260723t000500000z"
      },
      now: new Date("2000-01-01T00:00:00.000Z"),
      leaseMs: 300_000
    };

    try {
      const first = await store.acquire(command);
      const serverNow = await pool.query<{ readonly now: Date }>(
        "SELECT statement_timestamp() AS now"
      );
      expect(first.status).toBe("acquired");
      expect(Math.abs(
        new Date(first.record.acquiredAt).getTime()
        - (serverNow.rows[0]?.now.getTime() ?? 0)
      )).toBeLessThan(10_000);
      expect(
        new Date(first.record.leaseExpiresAt).getTime()
        - new Date(first.record.acquiredAt).getTime()
      ).toBe(300_000);

      await pool.query(
        `UPDATE worker_uplift_scheduler.feed_leases
         SET expires_at = statement_timestamp()
         WHERE lease_token = $1`,
        [
          first.record.token
        ]
      );
      await expect(store.markConfirmed(
        first.record.token,
        new Date("2099-01-01T00:00:00.000Z"),
        "receipt-expired"
      )).rejects.toBeInstanceOf(ScheduleLeaseOwnershipError);

      for (const operation of ["renew", "release", "fail"] as const) {
        const boundary = await store.acquire({
          ...command,
          feedId: `${command.feedId}-${operation}`,
          idempotencyKey: `${command.idempotencyKey}-${operation}`
        });
        await pool.query(
          `UPDATE worker_uplift_scheduler.feed_leases
           SET expires_at = statement_timestamp()
           WHERE lease_token = $1`,
          [
            boundary.record.token
          ]
        );
        const mutation = operation === "renew"
          ? store.renew(boundary.record.token, 300_000)
          : operation === "release"
            ? store.release(boundary.record.token, new Date("2099-01-01T00:00:00.000Z"))
            : store.markFailed(
                boundary.record.token,
                new Date("2099-01-01T00:00:00.000Z"),
                "expired"
              );

        await expect(mutation).rejects.toBeInstanceOf(ScheduleLeaseOwnershipError);
      }

      const reclaimed = await store.acquire(command);
      expect(reclaimed.status).toBe("acquired");
      expect(reclaimed.record.token).not.toBe(first.record.token);
      expect(reclaimed.record.attemptCount).toBe(2);
      await expect(store.release(
        first.record.token,
        new Date("2099-01-01T00:00:00.000Z")
      )).rejects.toBeInstanceOf(ScheduleLeaseOwnershipError);

      const renewed = await store.renew(reclaimed.record.token, 300_000);
      const renewedNow = await pool.query<{ readonly now: Date }>(
        "SELECT statement_timestamp() AS now"
      );
      const renewedRemainingMs = new Date(renewed.leaseExpiresAt).getTime()
        - (renewedNow.rows[0]?.now.getTime() ?? 0);
      expect(renewedRemainingMs).toBeGreaterThan(299_000);
      expect(renewedRemainingMs).toBeLessThanOrEqual(300_000);

      const confirmed = await store.markConfirmed(
        reclaimed.record.token,
        new Date("2099-01-01T00:00:00.000Z"),
        "receipt-confirmed"
      );
      expect(confirmed.status).toBe("confirmed");
      expect(new Date(confirmed.confirmedAt ?? "").getUTCFullYear()).not.toBe(2099);
      await expect(store.release(
        reclaimed.record.token,
        new Date("2099-01-01T00:00:00.000Z")
      )).rejects.toBeInstanceOf(ScheduleLeaseOwnershipError);
    } finally {
      await store.close();
    }
  });

  it("serializes adjacent windows per feed without coupling idempotency across feeds", async () => {
    const store = new PostgresScheduleLeaseStore(postgresUrl ?? "");
    const firstWindow: ScheduleLeaseAcquireCommand = {
      feedId: "feed-adjacent-window",
      idempotencyKey: "shared-regression-key",
      window: {
        start: "2026-07-23T00:05:00.000Z",
        end: "2026-07-23T00:10:00.000Z",
        key: "20260723t000500000z"
      },
      now: new Date("2000-01-01T00:00:00.000Z"),
      leaseMs: 300_000
    };

    try {
      const first = await store.acquire(firstWindow);
      const adjacent = await store.acquire({
        ...firstWindow,
        idempotencyKey: "scheduler:feed:feed-adjacent-window:20260723t001000000z",
        window: {
          start: "2026-07-23T00:10:00.000Z",
          end: "2026-07-23T00:15:00.000Z",
          key: "20260723t001000000z"
        }
      });

      expect(first.status).toBe("acquired");
      expect(adjacent).toMatchObject({
        status: "lease_active",
        record: {
          token: first.record.token,
          idempotencyKey: firstWindow.idempotencyKey
        }
      });

      await store.markConfirmed(
        first.record.token,
        new Date("2099-01-01T00:00:00.000Z"),
        "receipt-first-feed"
      );
      const secondFeed = await store.acquire({
        ...firstWindow,
        feedId: "feed-cross-key-isolation"
      });

      expect(secondFeed).toMatchObject({
        status: "acquired",
        record: {
          feedId: "feed-cross-key-isolation",
          idempotencyKey: firstWindow.idempotencyKey,
          attemptCount: 1
        }
      });
      expect(secondFeed.record.token).not.toBe(first.record.token);
    } finally {
      await store.close();
    }
  });

  function createReplica(
    brokerTransport: RabbitMqTestTransport,
    uuids: readonly string[],
    feed: SchedulerFeedDefinition
  ) {
    const config = loadSchedulerConfig({
      NUTSNEWS_SCHEDULER_TELEMETRY_LOGS: "silent",
      NUTSNEWS_SCHEDULER_CONCURRENCY: "4"
    });
    const dependencies: SchedulerDependencies = {
      mode: "test",
      clockKind: "manual-test",
      brokerKind: "local-test",
      clock: new ManualSchedulerClock("2026-07-23T00:05:42.000Z"),
      feedSource: createLocalFeedSource({
        feeds: [
          feed
        ]
      }),
      leaseStore: new PostgresScheduleLeaseStore(postgresUrl ?? ""),
      brokerTransport,
      brokerProbe: brokerTransport
    };

    return {
      service: createSchedulerService({
        config,
        dependencies,
        idFactory: new SequenceSchedulerIdFactory(uuids)
      })
    };
  }
});

export class PostgresTestLeaseStore implements ScheduleLeaseStore {
  readonly name = "postgres-test-lease-store";
  readonly adapterKind = "postgres" as const;

  constructor(private readonly pool: Pool) {}

  probe(): Promise<{ readonly status: "ok"; readonly summary: string }> {
    return Promise.resolve({
      status: "ok",
      summary: "PostgreSQL test lease store ready"
    });
  }

  async acquire(command: ScheduleLeaseAcquireCommand): Promise<ScheduleLeaseAcquireResult> {
    const client = await this.pool.connect();

    try {
      await client.query("BEGIN");
      const inserted = await client.query<DbLeaseRow>(
        `INSERT INTO scheduler_test_leases (
          idempotency_key,
          token,
          feed_id,
          window_start,
          window_end,
          status,
          acquired_at,
          lease_expires_at,
          attempt_count
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        ON CONFLICT (idempotency_key) DO NOTHING
        RETURNING *`,
        [
          command.idempotencyKey,
          `${command.idempotencyKey}:1`,
          command.feedId,
          command.window.start,
          command.window.end,
          "leased",
          command.now.toISOString(),
          new Date(command.now.getTime() + command.leaseMs).toISOString(),
          1
        ]
      );
      const insertedRow = inserted.rows[0];

      if (insertedRow !== undefined) {
        await client.query("COMMIT");
        return {
          status: "acquired",
          record: rowToRecord(insertedRow)
        };
      }

      const existing = await client.query<DbLeaseRow>(
        "SELECT * FROM scheduler_test_leases WHERE idempotency_key = $1 FOR UPDATE",
        [
          command.idempotencyKey
        ]
      );
      const row = oneRow(existing);

      const record = rowToRecord(row);

      if (record.status === "confirmed") {
        await client.query("COMMIT");
        return {
          status: "already_confirmed",
          record
        };
      }

      if (record.status === "leased" && Date.parse(record.leaseExpiresAt) > command.now.getTime()) {
        await client.query("COMMIT");
        return {
          status: "lease_active",
          record
        };
      }

      const attemptCount = row.attempt_count + 1;
      const token = `${command.idempotencyKey}:${String(attemptCount)}`;
      const updated = await client.query<DbLeaseRow>(
        `UPDATE scheduler_test_leases
         SET
          token = $1,
          status = 'leased',
          acquired_at = $2,
          lease_expires_at = $3,
          attempt_count = $4,
          confirmed_at = NULL,
          failed_at = NULL,
          failure_reason = NULL,
          publish_receipt_message_id = NULL
         WHERE idempotency_key = $5
         RETURNING *`,
        [
          token,
          command.now.toISOString(),
          new Date(command.now.getTime() + command.leaseMs).toISOString(),
          attemptCount,
          command.idempotencyKey
        ]
      );
      await client.query("COMMIT");

      return {
        status: "acquired",
        record: rowToRecord(oneRow(updated))
      };
    } catch (error: unknown) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async markConfirmed(token: string, confirmedAt: Date, publishReceiptMessageId: string): Promise<ScheduleLeaseRecord> {
    const result = await this.pool.query<DbLeaseRow>(
      `UPDATE scheduler_test_leases
       SET status = 'confirmed', confirmed_at = $2, publish_receipt_message_id = $3
       WHERE token = $1
       RETURNING *`,
      [
        token,
        confirmedAt.toISOString(),
        publishReceiptMessageId
      ]
    );

    return rowToRecord(oneRow(result));
  }

  renew(_token: string, _leaseMs: number): Promise<ScheduleLeaseRecord> {
    void _token;
    void _leaseMs;
    return Promise.reject(new Error("PostgresTestLeaseStore renewal is not used by this integration path."));
  }

  release(_token: string, _releasedAt: Date): Promise<ScheduleLeaseRecord> {
    void _token;
    void _releasedAt;
    return Promise.reject(new Error("PostgresTestLeaseStore release is not used by this integration path."));
  }

  async markFailed(token: string, failedAt: Date, failureReason: string): Promise<ScheduleLeaseRecord> {
    const result = await this.pool.query<DbLeaseRow>(
      `UPDATE scheduler_test_leases
       SET status = 'failed', failed_at = $2, failure_reason = $3
       WHERE token = $1
       RETURNING *`,
      [
        token,
        failedAt.toISOString(),
        failureReason
      ]
    );

    return rowToRecord(oneRow(result));
  }

  async get(idempotencyKey: string): Promise<ScheduleLeaseRecord | undefined> {
    const result = await this.pool.query<DbLeaseRow>(
      "SELECT * FROM scheduler_test_leases WHERE idempotency_key = $1",
      [
        idempotencyKey
      ]
    );
    const row = result.rows[0];

    return row === undefined ? undefined : rowToRecord(row);
  }

  close(): Promise<void> {
    return Promise.resolve();
  }
}

interface DbLeaseRow {
  readonly idempotency_key: string;
  readonly token: string;
  readonly feed_id: string;
  readonly window_start: Date;
  readonly window_end: Date;
  readonly status: ScheduleLeaseRecord["status"];
  readonly acquired_at: Date;
  readonly lease_expires_at: Date;
  readonly attempt_count: number;
  readonly confirmed_at: Date | null;
  readonly failed_at: Date | null;
  readonly failure_reason: string | null;
  readonly publish_receipt_message_id: string | null;
}

function oneRow(result: QueryResult<DbLeaseRow>): DbLeaseRow {
  const row = result.rows[0];

  if (row === undefined) {
    throw new Error("expected one database row");
  }

  return row;
}

function rowToRecord(row: DbLeaseRow): ScheduleLeaseRecord {
  return {
    token: row.token,
    feedId: row.feed_id,
    idempotencyKey: row.idempotency_key,
    window: {
      start: row.window_start.toISOString(),
      end: row.window_end.toISOString(),
      key: row.idempotency_key.split(":").at(-1) ?? "unknown"
    },
    status: row.status,
    acquiredAt: row.acquired_at.toISOString(),
    leaseExpiresAt: row.lease_expires_at.toISOString(),
    attemptCount: row.attempt_count,
    ...(row.confirmed_at === null ? {} : {
      confirmedAt: row.confirmed_at.toISOString()
    }),
    ...(row.failed_at === null ? {} : {
      failedAt: row.failed_at.toISOString()
    }),
    ...(row.failure_reason === null ? {} : {
      failureReason: row.failure_reason
    }),
    ...(row.publish_receipt_message_id === null ? {} : {
      publishReceiptMessageId: row.publish_receipt_message_id
    })
  };
}

class RabbitMqTestTransport implements RuntimeBrokerTransport {
  readonly name = "rabbitmq-test-transport";
  private adminConnection: ChannelModel | undefined;
  private adminChannel: ConfirmChannel | undefined;
  private publisher: SchedulerRabbitMqPublisherTransport | undefined;
  private route = getWorkerRoute("fetch");

  constructor(private readonly url: string | undefined) {}

  async probe() {
    return this.requirePublisher().probe();
  }

  async connect(): Promise<void> {
    if (this.url === undefined) {
      throw new Error("RabbitMQ URL is required");
    }

    this.publisher = new SchedulerRabbitMqPublisherTransport({
      url: this.url
    });
    this.adminConnection = await amqp.connect(this.url);
    this.adminChannel = await this.adminConnection.createConfirmChannel();
    await this.publisher.connect();
  }

  async assertTopology(routes: readonly WorkerRoute[]): Promise<void> {
    const channel = this.requireChannel();

    for (const route of routes) {
      this.route = route;
      await channel.assertExchange(route.exchange, "topic", {
        durable: true
      });
      await channel.assertQueue(route.mainQueue.name, {
        durable: true
      });
      await channel.purgeQueue(route.mainQueue.name);
      await channel.bindQueue(route.mainQueue.name, route.exchange, route.routingKey);
    }
  }

  publish(command: BrokerPublishCommand): Promise<BrokerPublishReceipt> {
    return this.requirePublisher().publish(command);
  }

  consume(stage: WorkerStage, handler: BrokerDeliveryHandler): Promise<never> {
    void stage;
    void handler;
    return Promise.reject(new Error("consume is not used by scheduler integration tests"));
  }

  drain(): Promise<void> {
    return this.requirePublisher().drain();
  }

  async close(): Promise<void> {
    const channel = this.adminChannel;
    const connection = this.adminConnection;
    const publisher = this.publisher;

    this.adminChannel = undefined;
    this.adminConnection = undefined;
    this.publisher = undefined;

    if (publisher !== undefined) {
      await publisher.close();
    }

    if (channel !== undefined) {
      await closeRabbitMqResource(() => channel.close());
    }

    if (connection !== undefined) {
      await closeRabbitMqResource(() => connection.close());
    }
  }

  async queueMessageCount(): Promise<number> {
    const result = await this.requireChannel().checkQueue(this.route.mainQueue.name);
    return result.messageCount;
  }

  private requireChannel(): ConfirmChannel {
    if (this.adminChannel === undefined) {
      throw new Error("RabbitMQ channel is not connected");
    }

    return this.adminChannel;
  }

  private requirePublisher(): SchedulerRabbitMqPublisherTransport {
    if (this.publisher === undefined) {
      throw new Error("RabbitMQ publisher is not connected");
    }

    return this.publisher;
  }
}

async function closeRabbitMqResource(close: () => Promise<void>): Promise<void> {
  try {
    await close();
  } catch (error: unknown) {
    if (!isRabbitMqAlreadyClosedError(error)) {
      throw error;
    }
  }
}

function isRabbitMqAlreadyClosedError(error: unknown): boolean {
  return error instanceof Error && /(?:channel|connection) closed/i.test(error.message);
}

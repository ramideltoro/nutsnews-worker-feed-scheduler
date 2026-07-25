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
    await pool.query(`
      CREATE TABLE IF NOT EXISTS scheduler_test_leases (
        idempotency_key text PRIMARY KEY,
        token text NOT NULL,
        feed_id text NOT NULL,
        window_start timestamptz NOT NULL,
        window_end timestamptz NOT NULL,
        status text NOT NULL,
        acquired_at timestamptz NOT NULL,
        lease_expires_at timestamptz NOT NULL,
        attempt_count integer NOT NULL,
        confirmed_at timestamptz,
        failed_at timestamptz,
        failure_reason text,
        publish_receipt_message_id text
      )
    `);
    await pool.query("TRUNCATE scheduler_test_leases");
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
      clock: new ManualSchedulerClock("2026-07-23T00:05:42.000Z"),
      feedSource: createLocalFeedSource({
        feeds: [
          feed
        ]
      }),
      leaseStore: new PostgresTestLeaseStore(pool),
      brokerTransport
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

class PostgresTestLeaseStore implements ScheduleLeaseStore {
  constructor(private readonly pool: Pool) {}

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
  private connection: ChannelModel | undefined;
  private channel: ConfirmChannel | undefined;
  private route = getWorkerRoute("fetch");

  constructor(private readonly url: string | undefined) {}

  async connect(): Promise<void> {
    if (this.url === undefined) {
      throw new Error("RabbitMQ URL is required");
    }

    this.connection = await amqp.connect(this.url);
    this.channel = await this.connection.createConfirmChannel();
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
    const channel = this.requireChannel();
    const route = getWorkerRoute(command.envelope.route);
    const body = Buffer.from(JSON.stringify(command));

    return new Promise((resolve, reject) => {
      channel.publish(route.exchange, route.routingKey, body, {
        contentType: "application/json",
        persistent: true,
        mandatory: true
      }, (error) => {
        if (error !== null) {
          reject(error instanceof Error ? error : new Error("RabbitMQ publish confirm failed."));
          return;
        }

        resolve({
          messageId: command.envelope.messageId,
          stage: command.envelope.route,
          exchange: route.exchange,
          routingKey: route.routingKey,
          confirmed: true,
          confirmedAt: command.envelope.occurredAt
        });
      });
    });
  }

  consume(stage: WorkerStage, handler: BrokerDeliveryHandler): Promise<never> {
    void stage;
    void handler;
    return Promise.reject(new Error("consume is not used by scheduler integration tests"));
  }

  drain(): Promise<void> {
    return Promise.resolve();
  }

  async close(): Promise<void> {
    const channel = this.channel;
    const connection = this.connection;

    this.channel = undefined;
    this.connection = undefined;

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
    if (this.channel === undefined) {
      throw new Error("RabbitMQ channel is not connected");
    }

    return this.channel;
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

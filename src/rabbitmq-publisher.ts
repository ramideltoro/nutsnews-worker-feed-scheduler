import {
  connect as amqpConnect,
  type ChannelModel,
  type ConfirmChannel,
  type ConsumeMessage
} from "amqplib";
import {
  getWorkerRoute,
  type WorkerRoute,
  type WorkerStage
} from "@ramideltoro/nutsnews-worker-contracts";
import {
  runtimeNow,
  runtimeTraceHeadersFromEnvelope,
  SYSTEM_RUNTIME_CLOCK,
  type BrokerDeliveryHandler,
  type BrokerPublishCommand,
  type BrokerPublishReceipt,
  type RuntimeBrokerTransport
} from "@ramideltoro/nutsnews-worker-runtime";

import type {
  SchedulerBrokerProbe,
  SchedulerDependencyProbe
} from "./dependencies.js";

const DEFAULT_CONFIRM_TIMEOUT_MS = 5_000;
const DEFAULT_DRAIN_TIMEOUT_MS = 30_000;

export interface SchedulerRabbitMqPublisherOptions {
  readonly url: string;
  readonly confirmTimeoutMs?: number;
  readonly drainTimeoutMs?: number;
  readonly connect?: (url: string) => Promise<ChannelModel>;
}

export class SchedulerRabbitMqPublisherTransport
implements RuntimeBrokerTransport, SchedulerBrokerProbe {
  readonly name = "rabbitmq-payload-publisher";
  private readonly url: string;
  private readonly confirmTimeoutMs: number;
  private readonly drainTimeoutMs: number;
  private readonly connectToBroker: (url: string) => Promise<ChannelModel>;
  private readonly inFlight = new Set<Promise<unknown>>();
  private connection: ChannelModel | undefined;
  private channel: ConfirmChannel | undefined;
  private closing = false;

  constructor(options: SchedulerRabbitMqPublisherOptions) {
    this.url = options.url;
    this.confirmTimeoutMs = options.confirmTimeoutMs ?? DEFAULT_CONFIRM_TIMEOUT_MS;
    this.drainTimeoutMs = options.drainTimeoutMs ?? DEFAULT_DRAIN_TIMEOUT_MS;
    this.connectToBroker = options.connect ?? amqpConnect;
  }

  get inFlightDeliveryCount(): number {
    return this.inFlight.size;
  }

  async probe(): Promise<SchedulerDependencyProbe> {
    try {
      await this.ensureChannel();
      return {
        status: "ok",
        summary: "RabbitMQ publisher-confirm transport ready"
      };
    } catch (error: unknown) {
      return {
        status: "unhealthy",
        summary: `RabbitMQ publisher-confirm transport unavailable (${errorClass(error)})`
      };
    }
  }

  async connect(): Promise<void> {
    this.closing = false;
    await this.ensureChannel();
  }

  async assertTopology(_routes: readonly WorkerRoute[]): Promise<void> {
    void _routes;
    await this.ensureChannel();
  }

  publish(command: BrokerPublishCommand): Promise<BrokerPublishReceipt> {
    const operation = this.publishWithConfirm(command);
    this.inFlight.add(operation);
    void operation.finally(() => {
      this.inFlight.delete(operation);
    }).catch(() => undefined);
    return operation;
  }

  consume(_stage: WorkerStage, _handler: BrokerDeliveryHandler): Promise<never> {
    void _stage;
    void _handler;
    return Promise.reject(new Error("Scheduler RabbitMQ transport is publisher-only."));
  }

  async drain(timeoutMs = this.drainTimeoutMs): Promise<void> {
    if (this.inFlight.size === 0) {
      return;
    }

    await boundedWait(
      Promise.all([...this.inFlight]).then(() => undefined),
      timeoutMs,
      "RabbitMQ publisher drain exceeded its bounded timeout."
    );
  }

  async close(): Promise<void> {
    this.closing = true;
    await this.drain().catch(() => undefined);
    const channel = this.channel;
    const connection = this.connection;
    this.channel = undefined;
    this.connection = undefined;

    if (channel !== undefined) {
      await channel.close().catch(() => undefined);
    }

    if (connection !== undefined) {
      await connection.close().catch(() => undefined);
    }
  }

  private async publishWithConfirm(command: BrokerPublishCommand): Promise<BrokerPublishReceipt> {
    const channel = await this.ensureChannel();
    const route = getWorkerRoute(command.envelope.route);
    const content = Buffer.from(JSON.stringify({
      envelope: command.envelope,
      payload: command.payload
    }), "utf8");

    await new Promise<void>((resolve, reject) => {
      let settled = false;
      const finish = (error?: Error): void => {
        if (settled) {
          return;
        }

        settled = true;
        clearTimeout(timer);
        channel.off("return", onReturn);
        channel.off("close", onClose);
        channel.off("error", onError);

        if (error === undefined) {
          resolve();
        } else {
          reject(error);
        }
      };
      const onReturn = (message: ConsumeMessage): void => {
        if (message.properties.messageId === command.envelope.messageId) {
          finish(new Error("RabbitMQ returned the mandatory scheduler message as unroutable."));
        }
      };
      const onClose = (): void => {
        this.markDisconnected(channel);
        finish(new Error("RabbitMQ publisher channel closed before confirmation."));
      };
      const onError = (): void => {
        this.markDisconnected(channel);
        finish(new Error("RabbitMQ publisher channel failed before confirmation."));
      };
      const timer = setTimeout(() => {
        finish(new Error("RabbitMQ publisher confirmation timed out."));
      }, this.confirmTimeoutMs);

      channel.on("return", onReturn);
      channel.on("close", onClose);
      channel.on("error", onError);
      channel.publish(route.exchange, route.routingKey, content, {
        contentType: "application/json",
        deliveryMode: 2,
        mandatory: true,
        messageId: command.envelope.messageId,
        correlationId: command.envelope.correlationId,
        timestamp: Date.parse(command.envelope.occurredAt),
        headers: runtimeTraceHeadersFromEnvelope(command.envelope)
      }, (error) => {
        finish(error instanceof Error
          ? error
          : error === null || error === undefined
            ? undefined
            : new Error("RabbitMQ publisher confirmation failed."));
      });
    });

    return {
      messageId: command.envelope.messageId,
      stage: command.envelope.route,
      exchange: route.exchange,
      routingKey: route.routingKey,
      confirmed: true,
      confirmedAt: runtimeNow(SYSTEM_RUNTIME_CLOCK)
    };
  }

  private async ensureChannel(): Promise<ConfirmChannel> {
    if (this.closing) {
      throw new Error("RabbitMQ publisher transport is closing.");
    }

    if (this.channel !== undefined) {
      return this.channel;
    }

    const connection = await this.connectToBroker(this.url);
    const channel = await connection.createConfirmChannel();
    connection.on("close", () => {
      this.markDisconnected(channel);
    });
    connection.on("error", () => {
      this.markDisconnected(channel);
    });
    channel.on("close", () => {
      this.markDisconnected(channel);
    });
    channel.on("error", () => {
      this.markDisconnected(channel);
    });
    this.connection = connection;
    this.channel = channel;
    return channel;
  }

  private markDisconnected(channel: ConfirmChannel): void {
    if (this.channel !== channel) {
      return;
    }

    this.channel = undefined;
    this.connection = undefined;
  }
}

function errorClass(error: unknown): string {
  return error instanceof Error && error.name.length > 0 ? error.name : "UnknownError";
}

async function boundedWait(
  operation: Promise<void>,
  timeoutMs: number,
  message: string
): Promise<void> {
  let timeout: ReturnType<typeof setTimeout> | undefined;

  try {
    await Promise.race([
      operation,
      new Promise<void>((_resolve, reject) => {
        timeout = setTimeout(() => {
          reject(new Error(message));
        }, timeoutMs);
      })
    ]);
  } finally {
    if (timeout !== undefined) {
      clearTimeout(timeout);
    }
  }
}

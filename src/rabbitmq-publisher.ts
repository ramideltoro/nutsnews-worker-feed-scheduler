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
import { SchedulerPublishError } from "./publish-error.js";

const DEFAULT_CONFIRM_TIMEOUT_MS = 5_000;
const DEFAULT_DRAIN_TIMEOUT_MS = 30_000;

export interface SchedulerRabbitMqPublisherOptions {
  readonly url: string;
  readonly confirmTimeoutMs?: number;
  readonly connectionTimeoutMs?: number;
  readonly drainTimeoutMs?: number;
  readonly connect?: (url: string) => Promise<ChannelModel>;
}

export class SchedulerRabbitMqPublisherTransport
implements RuntimeBrokerTransport, SchedulerBrokerProbe {
  readonly name = "rabbitmq-payload-publisher";
  private readonly url: string;
  private readonly confirmTimeoutMs: number;
  private readonly connectionTimeoutMs: number;
  private readonly drainTimeoutMs: number;
  private readonly connectToBroker: (url: string) => Promise<ChannelModel>;
  private readonly inFlight = new Set<Promise<unknown>>();
  private connection: ChannelModel | undefined;
  private channel: ConfirmChannel | undefined;
  private channelPromise: Promise<ConfirmChannel> | undefined;
  private closePromise: Promise<void> | undefined;
  private lifecycleGeneration = 0;
  private closing = false;

  constructor(options: SchedulerRabbitMqPublisherOptions) {
    this.url = options.url;
    this.confirmTimeoutMs = options.confirmTimeoutMs ?? DEFAULT_CONFIRM_TIMEOUT_MS;
    this.connectionTimeoutMs = options.connectionTimeoutMs ?? this.confirmTimeoutMs;
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
    const pendingClose = this.closePromise;

    if (pendingClose !== undefined) {
      await pendingClose;
    }

    if (this.closing) {
      this.closing = false;
      this.lifecycleGeneration += 1;
    }

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
      Promise.allSettled([...this.inFlight]).then(() => undefined),
      timeoutMs,
      "RabbitMQ publisher drain exceeded its bounded timeout."
    );
  }

  async close(): Promise<void> {
    if (this.closePromise !== undefined) {
      return this.closePromise;
    }

    const operation = this.closeTransport();

    this.closePromise = operation;

    try {
      await operation;
    } finally {
      if (this.closePromise === operation) {
        this.closePromise = undefined;
      }
    }
  }

  private async closeTransport(): Promise<void> {
    this.closing = true;
    this.lifecycleGeneration += 1;
    await this.drain().catch(() => undefined);
    await this.channelPromise?.catch(() => undefined);
    const channel = this.channel;
    const connection = this.connection;
    this.channel = undefined;
    this.connection = undefined;

    await boundedClose(channel, connection, this.connectionTimeoutMs);
  }

  private async publishWithConfirm(command: BrokerPublishCommand): Promise<BrokerPublishReceipt> {
    let channel: ConfirmChannel;

    try {
      channel = await this.ensureChannel();
    } catch (error: unknown) {
      throw new SchedulerPublishError(
        `RabbitMQ was unavailable before publication (${errorClass(error)}).`,
        "not-published",
        {
          cause: error
        }
      );
    }

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
          finish(new SchedulerPublishError(
            "RabbitMQ returned the mandatory scheduler message as unroutable.",
            "rejected"
          ));
        }
      };
      const onClose = (): void => {
        void this.quarantineChannel(channel);
        finish(new SchedulerPublishError(
          "RabbitMQ publisher channel closed before confirmation.",
          "ambiguous"
        ));
      };
      const onError = (): void => {
        void this.quarantineChannel(channel);
        finish(new SchedulerPublishError(
          "RabbitMQ publisher channel failed before confirmation.",
          "ambiguous"
        ));
      };
      const timer = setTimeout(() => {
        finish(new SchedulerPublishError(
          "RabbitMQ publisher confirmation timed out.",
          "ambiguous"
        ));
        void this.quarantineChannel(channel);
      }, this.confirmTimeoutMs);

      channel.on("return", onReturn);
      channel.on("close", onClose);
      channel.on("error", onError);
      try {
        channel.publish(route.exchange, route.routingKey, content, {
          contentType: "application/json",
          deliveryMode: 2,
          mandatory: true,
          messageId: command.envelope.messageId,
          correlationId: command.envelope.correlationId,
          timestamp: Date.parse(command.envelope.occurredAt),
          headers: runtimeTraceHeadersFromEnvelope(command.envelope)
        }, (error) => {
          if (error === null || error === undefined) {
            finish();
            return;
          }

          finish(new SchedulerPublishError(
            error instanceof Error ? error.message : "RabbitMQ publisher confirmation failed.",
            "ambiguous",
            error instanceof Error ? {
              cause: error
            } : {}
          ));
          void this.quarantineChannel(channel);
        });
      } catch (error: unknown) {
        finish(new SchedulerPublishError(
          "RabbitMQ rejected publication before accepting the message.",
          "not-published",
          {
            cause: error
          }
        ));
        void this.quarantineChannel(channel);
      }
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

    if (this.channelPromise !== undefined) {
      return this.channelPromise;
    }

    const generation = this.lifecycleGeneration;
    const operation = this.createChannel(generation);

    this.channelPromise = operation;

    try {
      return await operation;
    } finally {
      if (this.channelPromise === operation) {
        this.channelPromise = undefined;
      }
    }
  }

  private async createChannel(generation: number): Promise<ConfirmChannel> {
    const connection = await boundedValue(
      Promise.resolve().then(() => this.connectToBroker(this.url)),
      this.connectionTimeoutMs,
      "RabbitMQ publisher connection exceeded its bounded timeout.",
      (lateConnection) => {
        void lateConnection.close().catch(() => undefined);
      }
    );
    const setupFailure = observeConnectionSetupFailure(connection);

    if (!this.isCurrentGeneration(generation)) {
      setupFailure.stop();
      await boundedClose(undefined, connection, this.connectionTimeoutMs);
      throw new Error("RabbitMQ publisher connection completed after shutdown began.");
    }

    let channel: ConfirmChannel;

    try {
      channel = await boundedValue(
        Promise.race([
          Promise.resolve().then(() => connection.createConfirmChannel()),
          setupFailure.promise
        ]),
        this.connectionTimeoutMs,
        "RabbitMQ confirm-channel creation exceeded its bounded timeout.",
        (lateChannel) => {
          void lateChannel.close().catch(() => undefined);
          void connection.close().catch(() => undefined);
        }
      );
    } catch (error: unknown) {
      setupFailure.stop();
      await boundedClose(undefined, connection, this.connectionTimeoutMs);
      throw error;
    }

    if (!this.isCurrentGeneration(generation)) {
      setupFailure.stop();
      await boundedClose(channel, connection, this.connectionTimeoutMs);
      throw new Error("RabbitMQ confirm channel completed after shutdown began.");
    }

    const setupError = setupFailure.error;

    if (setupError !== undefined) {
      setupFailure.stop();
      await boundedClose(channel, connection, this.connectionTimeoutMs);
      throw setupError;
    }

    connection.on("close", () => {
      void this.quarantineChannel(channel);
    });
    connection.on("error", () => {
      void this.quarantineChannel(channel);
    });
    channel.on("close", () => {
      void this.quarantineChannel(channel);
    });
    channel.on("error", () => {
      void this.quarantineChannel(channel);
    });
    setupFailure.stop();
    this.connection = connection;
    this.channel = channel;
    return channel;
  }

  private isCurrentGeneration(generation: number): boolean {
    return !this.closing && generation === this.lifecycleGeneration;
  }

  private async quarantineChannel(channel: ConfirmChannel): Promise<void> {
    if (this.channel !== channel) {
      return;
    }

    const connection = this.connection;

    this.channel = undefined;
    this.connection = undefined;

    await boundedClose(channel, connection, this.connectionTimeoutMs);
  }
}

function errorClass(error: unknown): string {
  return error instanceof Error && error.name.length > 0 ? error.name : "UnknownError";
}

interface ConnectionSetupFailure {
  readonly promise: Promise<never>;
  readonly error: Error | undefined;
  stop(): void;
}

function observeConnectionSetupFailure(connection: ChannelModel): ConnectionSetupFailure {
  let setupError: Error | undefined;
  let rejectFailure: ((error: Error) => void) | undefined;
  const promise = new Promise<never>((_resolve, reject) => {
    rejectFailure = reject;
  });
  const fail = (error: Error): void => {
    if (setupError !== undefined) {
      return;
    }

    setupError = error;
    rejectFailure?.(error);
  };
  const onError = (error: unknown): void => {
    fail(error instanceof Error
      ? error
      : new Error("RabbitMQ connection failed during confirm-channel setup."));
  };
  const onClose = (): void => {
    fail(new Error("RabbitMQ connection closed during confirm-channel setup."));
  };

  connection.on("error", onError);
  connection.on("close", onClose);

  return {
    promise,
    get error(): Error | undefined {
      return setupError;
    },
    stop: () => {
      connection.off("error", onError);
      connection.off("close", onClose);
    }
  };
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

async function boundedClose(
  channel: ConfirmChannel | undefined,
  connection: ChannelModel | undefined,
  timeoutMs: number
): Promise<void> {
  const closures = [
    closeQuietly(channel),
    closeQuietly(connection)
  ].filter((operation): operation is Promise<void> => operation !== undefined);

  if (closures.length === 0) {
    return;
  }

  await boundedWait(
    Promise.all(closures).then(() => undefined),
    timeoutMs,
    "RabbitMQ publisher resource close exceeded its bounded timeout."
  ).catch(() => undefined);
}

function closeQuietly(
  resource: { close(): Promise<void> } | undefined
): Promise<void> | undefined {
  return resource === undefined
    ? undefined
    : Promise.resolve()
        .then(() => resource.close())
        .catch(() => undefined);
}

async function boundedValue<T>(
  operation: Promise<T>,
  timeoutMs: number,
  message: string,
  onLateValue?: (value: T) => void
): Promise<T> {
  let timedOut = false;
  let timeout: ReturnType<typeof setTimeout> | undefined;

  void operation.then((value) => {
    if (timedOut) {
      onLateValue?.(value);
    }
  }).catch(() => undefined);

  try {
    return await Promise.race([
      operation,
      new Promise<T>((_resolve, reject) => {
        timeout = setTimeout(() => {
          timedOut = true;
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

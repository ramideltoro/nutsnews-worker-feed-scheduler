import { EventEmitter } from "node:events";

import type {
  ChannelModel,
  ConfirmChannel
} from "amqplib";
import type { BrokerPublishCommand } from "@ramideltoro/nutsnews-worker-runtime";
import {
  afterEach,
  describe,
  expect,
  it,
  vi
} from "vitest";

import { SchedulerPublishError } from "../src/publish-error.js";
import { SchedulerRabbitMqPublisherTransport } from "../src/rabbitmq-publisher.js";
import { createMinimalFetchEnvelope } from "../src/test-doubles.js";

describe("scheduler RabbitMQ publisher", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("publishes the complete fetch carrier with mandatory persistent confirmation", async () => {
    const published: PublishedMessage[] = [];
    const channel = createConfirmChannel((message) => {
      published.push(message);
      message.callback?.(undefined);
    });
    const connect = vi.fn(() => Promise.resolve(createConnection(channel)));
    const transport = new SchedulerRabbitMqPublisherTransport({
      url: "amqps://scheduler.example.test/worker-uplift",
      connect
    });
    const command: BrokerPublishCommand = {
      envelope: createMinimalFetchEnvelope(),
      payload: {
        feedId: "feed-world",
        feedUrl: "https://feeds.example.test/world.xml",
        shadowMode: true
      }
    };

    await transport.connect();
    const receipt = await transport.publish(command);

    expect(connect).toHaveBeenCalledOnce();
    expect(published).toHaveLength(1);
    expect(JSON.parse(published[0]?.content.toString("utf8") ?? "null")).toEqual(command);
    expect(published[0]?.options).toMatchObject({
      contentType: "application/json",
      deliveryMode: 2,
      mandatory: true,
      messageId: command.envelope.messageId,
      correlationId: command.envelope.correlationId
    });
    expect(receipt).toMatchObject({
      messageId: command.envelope.messageId,
      stage: "fetch",
      confirmed: true
    });
    expect(transport.inFlightDeliveryCount).toBe(0);

    await transport.close();
  });

  it("treats every confirm-callback error as ambiguous and reconnects", async () => {
    const firstChannel = createConfirmChannel((message) => {
      message.callback?.(new Error("channel closed before confirmation"));
    });
    const secondChannel = createConfirmChannel((message) => {
      message.callback?.(undefined);
    });
    const connect = vi.fn()
      .mockResolvedValueOnce(createConnection(firstChannel))
      .mockResolvedValueOnce(createConnection(secondChannel));
    const transport = new SchedulerRabbitMqPublisherTransport({
      url: "amqps://scheduler.example.test/worker-uplift",
      connect
    });

    await expect(transport.publish({
      envelope: createMinimalFetchEnvelope(),
      payload: {
        shadowMode: true
      }
    })).rejects.toMatchObject({
      name: "SchedulerPublishError",
      disposition: "ambiguous",
      message: "channel closed before confirmation"
    });

    await expect(transport.probe()).resolves.toMatchObject({
      status: "ok"
    });
    expect(connect).toHaveBeenCalledTimes(2);
  });

  it("uses a mandatory return as the definitive broker rejection signal", async () => {
    const channel = createConfirmChannel((message) => {
      (channel as unknown as EventEmitter).emit("return", {
        properties: {
          messageId: message.options.messageId
        }
      });
      message.callback?.(undefined);
    });
    const transport = new SchedulerRabbitMqPublisherTransport({
      url: "amqps://scheduler.example.test/worker-uplift",
      connect: () => Promise.resolve(createConnection(channel))
    });

    await expect(transport.publish({
      envelope: createMinimalFetchEnvelope(),
      payload: {
        shadowMode: true
      }
    })).rejects.toMatchObject({
      name: "SchedulerPublishError",
      disposition: "rejected"
    });
  });

  it("bounds connection acquisition and classifies it as definitely unpublished", async () => {
    vi.useFakeTimers();
    const transport = new SchedulerRabbitMqPublisherTransport({
      url: "amqps://scheduler.example.test/worker-uplift",
      connectionTimeoutMs: 25,
      connect: () => new Promise<ChannelModel>(() => undefined)
    });
    const publication = transport.publish({
      envelope: createMinimalFetchEnvelope(),
      payload: {
        shadowMode: true
      }
    });

    await vi.advanceTimersByTimeAsync(25);
    await expect(publication).rejects.toMatchObject({
      name: "SchedulerPublishError",
      disposition: "not-published"
    });
  });

  it("keeps a confirmation timeout ambiguous and quarantines the cached channel", async () => {
    vi.useFakeTimers();
    const firstChannel = createConfirmChannel(() => undefined);
    const secondChannel = createConfirmChannel((message) => {
      message.callback?.(undefined);
    });
    const connect = vi.fn()
      .mockResolvedValueOnce(createConnection(firstChannel))
      .mockResolvedValueOnce(createConnection(secondChannel));
    const transport = new SchedulerRabbitMqPublisherTransport({
      url: "amqps://scheduler.example.test/worker-uplift",
      confirmTimeoutMs: 25,
      connect
    });
    const publication = transport.publish({
      envelope: createMinimalFetchEnvelope(),
      payload: {
        shadowMode: true
      }
    });

    await vi.advanceTimersByTimeAsync(25);
    await expect(publication).rejects.toBeInstanceOf(SchedulerPublishError);
    await expect(publication).rejects.toMatchObject({
      disposition: "ambiguous"
    });

    await expect(transport.publish({
      envelope: createMinimalFetchEnvelope({
        messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3621"
      }),
      payload: {
        shadowMode: true
      }
    })).resolves.toMatchObject({
      confirmed: true
    });
    expect(connect).toHaveBeenCalledTimes(2);
  });

  it("remains ambiguous when amqplib reports callback failure before channel close", async () => {
    const channel = createConfirmChannel((message) => {
      message.callback?.(new Error("channel closed"));
      (channel as unknown as EventEmitter).emit("close");
    });
    const transport = new SchedulerRabbitMqPublisherTransport({
      url: "amqps://scheduler.example.test/worker-uplift",
      connect: () => Promise.resolve(createConnection(channel))
    });

    await expect(transport.publish({
      envelope: createMinimalFetchEnvelope(),
      payload: {
        shadowMode: true
      }
    })).rejects.toMatchObject({
      name: "SchedulerPublishError",
      disposition: "ambiguous"
    });
  });

  it("shares one pending connection across connect, probe, and publish", async () => {
    const connection = deferred<ChannelModel>();
    const channel = createConfirmChannel((message) => {
      message.callback?.(undefined);
    });
    const connect = vi.fn(() => connection.promise);
    const transport = new SchedulerRabbitMqPublisherTransport({
      url: "amqps://scheduler.example.test/worker-uplift",
      connect
    });
    const connecting = transport.connect();
    const probing = transport.probe();
    const publication = transport.publish({
      envelope: createMinimalFetchEnvelope(),
      payload: {
        shadowMode: true
      }
    });

    await vi.waitFor(() => {
      expect(connect).toHaveBeenCalledOnce();
    });
    connection.resolve(createConnection(channel));

    await expect(connecting).resolves.toBeUndefined();
    await expect(probing).resolves.toMatchObject({
      status: "ok"
    });
    await expect(publication).resolves.toMatchObject({
      confirmed: true
    });
    expect(connect).toHaveBeenCalledOnce();
  });

  it("drains every publication when one rejects while another remains pending", async () => {
    const messages: PublishedMessage[] = [];
    const channel = createConfirmChannel((message) => {
      messages.push(message);
    });
    const transport = new SchedulerRabbitMqPublisherTransport({
      url: "amqps://scheduler.example.test/worker-uplift",
      connect: () => Promise.resolve(createConnection(channel))
    });
    const rejected = transport.publish({
      envelope: createMinimalFetchEnvelope({
        messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3622"
      }),
      payload: {
        shadowMode: true
      }
    });
    const pending = transport.publish({
      envelope: createMinimalFetchEnvelope({
        messageId: "018f1598-2dd5-7c4f-9f92-8f7a7f8b3623"
      }),
      payload: {
        shadowMode: true
      }
    });

    await vi.waitFor(() => {
      expect(messages).toHaveLength(2);
    });
    let closeSettled = false;
    const closing = transport.close().then(() => {
      closeSettled = true;
    });

    messages[0]?.callback?.(new Error("first publish failed"));
    await expect(rejected).rejects.toMatchObject({
      disposition: "ambiguous"
    });
    await new Promise<void>((resolve) => {
      setImmediate(resolve);
    });
    expect(closeSettled).toBe(false);

    messages[1]?.callback?.(undefined);
    await expect(pending).resolves.toMatchObject({
      confirmed: true
    });
    await closing;
    expect(closeSettled).toBe(true);
  });

  it("contains a connection error emitted during confirm-channel setup", async () => {
    const connection = new EventEmitter();
    const close = vi.fn(() => Promise.resolve());
    const model = Object.assign(connection, {
      createConfirmChannel: () => {
        connection.emit("error", new Error("handshake failed"));
        return new Promise<ConfirmChannel>(() => undefined);
      },
      close
    }) as unknown as ChannelModel;
    const transport = new SchedulerRabbitMqPublisherTransport({
      url: "amqps://scheduler.example.test/worker-uplift",
      connect: () => Promise.resolve(model)
    });

    await expect(transport.probe()).resolves.toMatchObject({
      status: "unhealthy",
      summary: "RabbitMQ publisher-confirm transport unavailable (Error)"
    });
    expect(close).toHaveBeenCalledOnce();
  });

  it("fences an immediate connect followed by close in the same turn", async () => {
    const pendingConnection = deferred<ChannelModel>();
    const closeConnection = vi.fn(() => Promise.resolve());
    const connect = vi.fn(() => pendingConnection.promise);
    const transport = new SchedulerRabbitMqPublisherTransport({
      url: "amqps://scheduler.example.test/worker-uplift",
      connect
    });
    const connecting = transport.connect();
    const rejectedConnection = expect(connecting).rejects.toThrow(/shutdown began/u);
    const closing = transport.close();
    pendingConnection.resolve(createConnection(
      createConfirmChannel(() => undefined),
      closeConnection
    ));

    await rejectedConnection;
    await closing;
    expect(connect).toHaveBeenCalledOnce();
    expect(closeConnection).toHaveBeenCalledOnce();
    await expect(transport.probe()).resolves.toMatchObject({
      status: "unhealthy"
    });
  });

  it("returns a value-free unhealthy probe when connection fails", async () => {
    const transport = new SchedulerRabbitMqPublisherTransport({
      url: "amqps://scheduler:do-not-disclose@example.test/worker-uplift",
      connect: () => Promise.reject(new Error("amqps://scheduler:do-not-disclose@example.test"))
    });

    const probe = await transport.probe();

    expect(probe).toEqual({
      status: "unhealthy",
      summary: "RabbitMQ publisher-confirm transport unavailable (Error)"
    });
    expect(JSON.stringify(probe)).not.toContain("do-not-disclose");
  });
});

interface PublishedMessage {
  readonly exchange: string;
  readonly routingKey: string;
  readonly content: Buffer;
  readonly options: Readonly<Record<string, unknown>>;
  readonly callback?: (error: unknown) => void;
}

function createConfirmChannel(
  onPublish: (message: PublishedMessage) => void
): ConfirmChannel {
  const emitter = new EventEmitter();
  const publish = (
    exchange: string,
    routingKey: string,
    content: Buffer,
    options: Readonly<Record<string, unknown>> = {},
    callback?: (error: unknown) => void
  ): boolean => {
    onPublish({
      exchange,
      routingKey,
      content,
      options,
      ...(callback === undefined ? {} : {
        callback
      })
    });
    return true;
  };

  return Object.assign(emitter, {
    publish,
    close: () => Promise.resolve(),
    waitForConfirms: () => Promise.resolve()
  }) as unknown as ConfirmChannel;
}

function createConnection(
  channel: ConfirmChannel,
  close: () => Promise<void> = () => Promise.resolve()
): ChannelModel {
  return Object.assign(new EventEmitter(), {
    createConfirmChannel: () => Promise.resolve(channel),
    close
  }) as unknown as ChannelModel;
}

interface Deferred<T> {
  readonly promise: Promise<T>;
  resolve(value: T): void;
}

function deferred<T>(): Deferred<T> {
  let resolveValue: ((value: T) => void) | undefined;
  const promise = new Promise<T>((resolve) => {
    resolveValue = resolve;
  });

  return {
    promise,
    resolve: (value) => {
      resolveValue?.(value);
    }
  };
}

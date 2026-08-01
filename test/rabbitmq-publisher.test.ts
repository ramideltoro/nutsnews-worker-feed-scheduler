import { EventEmitter } from "node:events";

import type {
  ChannelModel,
  ConfirmChannel
} from "amqplib";
import type { BrokerPublishCommand } from "@ramideltoro/nutsnews-worker-runtime";
import {
  describe,
  expect,
  it,
  vi
} from "vitest";

import { SchedulerRabbitMqPublisherTransport } from "../src/rabbitmq-publisher.js";
import { createMinimalFetchEnvelope } from "../src/test-doubles.js";

describe("scheduler RabbitMQ publisher", () => {
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

  it("fails closed on a negative publisher confirmation", async () => {
    const channel = createConfirmChannel((message) => {
      message.callback?.(new Error("broker rejected the publish"));
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
    })).rejects.toThrow("broker rejected the publish");
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

function createConnection(channel: ConfirmChannel): ChannelModel {
  return Object.assign(new EventEmitter(), {
    createConfirmChannel: () => Promise.resolve(channel),
    close: () => Promise.resolve()
  }) as unknown as ChannelModel;
}

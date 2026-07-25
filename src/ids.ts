import crypto from "node:crypto";

export interface SchedulerIdFactory {
  uuid(): string;
  traceparent(): string;
}

export function createCryptoSchedulerIdFactory(): SchedulerIdFactory {
  return {
    uuid: () => crypto.randomUUID(),
    traceparent: () => {
      const traceId = crypto.randomBytes(16).toString("hex");
      const spanId = crypto.randomBytes(8).toString("hex");

      return `00-${traceId}-${spanId}-01`;
    }
  };
}

export class SequenceSchedulerIdFactory implements SchedulerIdFactory {
  private uuidIndex = 0;
  private traceIndex = 0;

  constructor(
    private readonly uuids: readonly string[],
    private readonly traceparents: readonly string[] = [
      "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    ]
  ) {}

  uuid(): string {
    const value = this.uuids[this.uuidIndex];

    if (value === undefined) {
      throw new Error("SequenceSchedulerIdFactory has no remaining UUIDs.");
    }

    this.uuidIndex += 1;
    return value;
  }

  traceparent(): string {
    const value = this.traceparents[Math.min(this.traceIndex, this.traceparents.length - 1)];

    if (value === undefined) {
      throw new Error("SequenceSchedulerIdFactory has no traceparent values.");
    }

    this.traceIndex += 1;
    return value;
  }
}

import type { SchedulerWindow } from "./scheduling.js";

export type ScheduleLeaseStatus = "leased" | "confirmed" | "failed";
export type ScheduleLeaseAcquireStatus = "acquired" | "already_confirmed" | "lease_active";

export interface ScheduleLeaseAcquireCommand {
  readonly feedId: string;
  readonly idempotencyKey: string;
  readonly window: SchedulerWindow;
  readonly now: Date;
  readonly leaseMs: number;
}

export interface ScheduleLeaseRecord {
  readonly token: string;
  readonly feedId: string;
  readonly idempotencyKey: string;
  readonly window: SchedulerWindow;
  readonly status: ScheduleLeaseStatus;
  readonly acquiredAt: string;
  readonly leaseExpiresAt: string;
  readonly attemptCount: number;
  readonly confirmedAt?: string;
  readonly failedAt?: string;
  readonly failureReason?: string;
  readonly publishReceiptMessageId?: string;
}

export type ScheduleLeaseAcquireResult =
  | {
      readonly status: "acquired";
      readonly record: ScheduleLeaseRecord;
    }
  | {
      readonly status: Exclude<ScheduleLeaseAcquireStatus, "acquired">;
      readonly record: ScheduleLeaseRecord;
    };

export interface ScheduleLeaseStore {
  acquire(command: ScheduleLeaseAcquireCommand): Promise<ScheduleLeaseAcquireResult>;
  markConfirmed(token: string, confirmedAt: Date, publishReceiptMessageId: string): Promise<ScheduleLeaseRecord>;
  markFailed(token: string, failedAt: Date, failureReason: string): Promise<ScheduleLeaseRecord>;
  get(idempotencyKey: string): Promise<ScheduleLeaseRecord | undefined>;
}

export class InMemoryScheduleLeaseStore implements ScheduleLeaseStore {
  private readonly records = new Map<string, ScheduleLeaseRecord>();
  private tokenCounter = 0;

  acquire(command: ScheduleLeaseAcquireCommand): Promise<ScheduleLeaseAcquireResult> {
    const existing = this.records.get(command.idempotencyKey);

    if (existing?.status === "confirmed") {
      return Promise.resolve({
        status: "already_confirmed",
        record: existing
      });
    }

    if (existing?.status === "leased" && Date.parse(existing.leaseExpiresAt) > command.now.getTime()) {
      return Promise.resolve({
        status: "lease_active",
        record: existing
      });
    }

    const attemptCount = existing === undefined ? 1 : existing.attemptCount + 1;
    const record: ScheduleLeaseRecord = {
      token: this.nextToken(),
      feedId: command.feedId,
      idempotencyKey: command.idempotencyKey,
      window: command.window,
      status: "leased",
      acquiredAt: command.now.toISOString(),
      leaseExpiresAt: new Date(command.now.getTime() + command.leaseMs).toISOString(),
      attemptCount
    };

    this.records.set(command.idempotencyKey, record);

    return Promise.resolve({
      status: "acquired",
      record
    });
  }

  markConfirmed(token: string, confirmedAt: Date, publishReceiptMessageId: string): Promise<ScheduleLeaseRecord> {
    const [key, existing] = this.findByToken(token);
    const record: ScheduleLeaseRecord = {
      ...existing,
      status: "confirmed",
      confirmedAt: confirmedAt.toISOString(),
      publishReceiptMessageId
    };

    this.records.set(key, record);
    return Promise.resolve(record);
  }

  markFailed(token: string, failedAt: Date, failureReason: string): Promise<ScheduleLeaseRecord> {
    const [key, existing] = this.findByToken(token);
    const record: ScheduleLeaseRecord = {
      ...existing,
      status: "failed",
      failedAt: failedAt.toISOString(),
      failureReason
    };

    this.records.set(key, record);
    return Promise.resolve(record);
  }

  get(idempotencyKey: string): Promise<ScheduleLeaseRecord | undefined> {
    return Promise.resolve(this.records.get(idempotencyKey));
  }

  private nextToken(): string {
    this.tokenCounter += 1;
    return `local-lease-${String(this.tokenCounter)}`;
  }

  private findByToken(token: string): readonly [string, ScheduleLeaseRecord] {
    for (const entry of this.records.entries()) {
      if (entry[1].token === token) {
        return entry;
      }
    }

    throw new Error(`Unknown schedule lease token ${token}.`);
  }
}

import type { SchedulerWindow } from "./scheduling.js";
import type { SchedulerDependencyProbe } from "./dependencies.js";

export const SCHEDULE_LEASE_MAX_MS = 300_000;

export type ScheduleLeaseStatus = "leased" | "confirmed" | "failed" | "released" | "expired";
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
  readonly ownershipCheckedAt?: string;
  readonly attemptCount: number;
  readonly confirmedAt?: string;
  readonly failedAt?: string;
  readonly releasedAt?: string;
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
  readonly name: string;
  readonly adapterKind: "in-memory-test" | "postgres";
  probe(): Promise<SchedulerDependencyProbe>;
  acquire(command: ScheduleLeaseAcquireCommand): Promise<ScheduleLeaseAcquireResult>;
  renew(token: string, leaseMs: number): Promise<ScheduleLeaseRecord>;
  release(token: string, releasedAt: Date): Promise<ScheduleLeaseRecord>;
  markConfirmed(token: string, confirmedAt: Date, publishReceiptMessageId: string): Promise<ScheduleLeaseRecord>;
  markFailed(token: string, failedAt: Date, failureReason: string): Promise<ScheduleLeaseRecord>;
  get(idempotencyKey: string): Promise<ScheduleLeaseRecord | undefined>;
  close(): Promise<void>;
}

export class InMemoryScheduleLeaseStore implements ScheduleLeaseStore {
  readonly name = "in-memory-test-lease-store";
  readonly adapterKind = "in-memory-test" as const;
  private readonly records = new Map<string, ScheduleLeaseRecord>();
  private tokenCounter = 0;

  constructor(private readonly now: () => Date = () => new Date()) {}

  probe(): Promise<SchedulerDependencyProbe> {
    return Promise.resolve({
      status: "ok",
      summary: "in-memory test lease store ready"
    });
  }

  acquire(command: ScheduleLeaseAcquireCommand): Promise<ScheduleLeaseAcquireResult> {
    assertScheduleLeaseDuration(command.leaseMs);
    const acquiredAt = this.now();
    const existing = this.records.get(command.idempotencyKey);

    if (existing?.status === "confirmed") {
      return Promise.resolve({
        status: "already_confirmed",
        record: existing
      });
    }

    if (existing?.status === "leased" && Date.parse(existing.leaseExpiresAt) > acquiredAt.getTime()) {
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
      acquiredAt: acquiredAt.toISOString(),
      leaseExpiresAt: new Date(acquiredAt.getTime() + command.leaseMs).toISOString(),
      ownershipCheckedAt: acquiredAt.toISOString(),
      attemptCount
    };

    this.records.set(command.idempotencyKey, record);

    return Promise.resolve({
      status: "acquired",
      record
    });
  }

  renew(token: string, leaseMs: number): Promise<ScheduleLeaseRecord> {
    assertScheduleLeaseDuration(leaseMs);
    const renewedAt = this.now();
    const [key, existing] = this.findUnexpiredLeasedToken(token, renewedAt);
    const record: ScheduleLeaseRecord = {
      ...existing,
      leaseExpiresAt: new Date(renewedAt.getTime() + leaseMs).toISOString(),
      ownershipCheckedAt: renewedAt.toISOString()
    };

    this.records.set(key, record);
    return Promise.resolve(record);
  }

  release(token: string, releasedAt: Date): Promise<ScheduleLeaseRecord> {
    void releasedAt;
    const operationAt = this.now();
    const [key, existing] = this.findUnexpiredLeasedToken(token, operationAt);
    const record: ScheduleLeaseRecord = {
      ...existing,
      status: "released",
      releasedAt: operationAt.toISOString()
    };

    this.records.set(key, record);
    return Promise.resolve(record);
  }

  markConfirmed(token: string, confirmedAt: Date, publishReceiptMessageId: string): Promise<ScheduleLeaseRecord> {
    void confirmedAt;
    const operationAt = this.now();
    const [key, existing] = this.findUnexpiredLeasedToken(token, operationAt);
    const record: ScheduleLeaseRecord = {
      ...existing,
      status: "confirmed",
      confirmedAt: operationAt.toISOString(),
      publishReceiptMessageId
    };

    this.records.set(key, record);
    return Promise.resolve(record);
  }

  markFailed(token: string, failedAt: Date, failureReason: string): Promise<ScheduleLeaseRecord> {
    void failedAt;
    const operationAt = this.now();
    const [key, existing] = this.findUnexpiredLeasedToken(token, operationAt);
    const record: ScheduleLeaseRecord = {
      ...existing,
      status: "failed",
      failedAt: operationAt.toISOString(),
      failureReason
    };

    this.records.set(key, record);
    return Promise.resolve(record);
  }

  get(idempotencyKey: string): Promise<ScheduleLeaseRecord | undefined> {
    return Promise.resolve(this.records.get(idempotencyKey));
  }

  close(): Promise<void> {
    return Promise.resolve();
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

    throw new ScheduleLeaseOwnershipError(`Unknown or stale schedule lease token ${token}.`);
  }

  private findUnexpiredLeasedToken(
    token: string,
    operationAt: Date
  ): readonly [string, ScheduleLeaseRecord] {
    const entry = this.findByToken(token);
    const record = entry[1];

    if (record.status !== "leased" || Date.parse(record.leaseExpiresAt) <= operationAt.getTime()) {
      throw new ScheduleLeaseOwnershipError("Schedule lease ownership is absent or expired.");
    }

    return entry;
  }
}

export class ScheduleLeaseOwnershipError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ScheduleLeaseOwnershipError";
  }
}

export function assertScheduleLeaseDuration(leaseMs: number): void {
  if (!Number.isSafeInteger(leaseMs) || leaseMs < 1_000 || leaseMs > SCHEDULE_LEASE_MAX_MS) {
    throw new RangeError(`Schedule lease duration must be an integer from 1000 through ${String(SCHEDULE_LEASE_MAX_MS)} milliseconds.`);
  }
}

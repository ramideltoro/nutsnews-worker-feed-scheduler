export type SchedulerPublishDisposition = "not-published" | "rejected" | "ambiguous";

export class SchedulerPublishError extends Error {
  readonly disposition: SchedulerPublishDisposition;

  constructor(
    message: string,
    disposition: SchedulerPublishDisposition,
    options: ErrorOptions = {}
  ) {
    super(message, options);
    this.name = "SchedulerPublishError";
    this.disposition = disposition;
  }
}

export function schedulerPublishDisposition(error: unknown): SchedulerPublishDisposition {
  return error instanceof SchedulerPublishError ? error.disposition : "ambiguous";
}

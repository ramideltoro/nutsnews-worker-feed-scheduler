import type { SchedulerService } from "./service.js";

export interface SchedulerLoopOptions {
  readonly service: SchedulerService;
  readonly cadenceMs: number;
  readonly initialDelayMs?: number;
  readonly setTimer?: (callback: () => void, delayMs: number) => NodeJS.Timeout;
  readonly clearTimer?: (timer: NodeJS.Timeout) => void;
  readonly onError?: (error: unknown) => void | Promise<void>;
}

export interface SchedulerLoop {
  readonly isRunning: boolean;
  start(): void;
  stop(): Promise<void>;
}

export function createSchedulerLoop(options: SchedulerLoopOptions): SchedulerLoop {
  const setTimer = options.setTimer ?? setTimeout;
  const clearTimer = options.clearTimer ?? clearTimeout;
  let running = false;
  let timer: NodeJS.Timeout | undefined;
  let inFlight: Promise<void> | undefined;

  const schedule = (delayMs: number): void => {
    if (!running) {
      return;
    }

    timer = setTimer(() => {
      timer = undefined;
      inFlight = runOnce();
    }, delayMs);
  };

  const runOnce = async (): Promise<void> => {
    try {
      await options.service.runOnce();
    } catch (error: unknown) {
      await options.onError?.(error);
    } finally {
      inFlight = undefined;
      schedule(options.cadenceMs);
    }
  };

  return {
    get isRunning(): boolean {
      return running;
    },
    start(): void {
      if (running) {
        return;
      }

      running = true;
      schedule(options.initialDelayMs ?? 0);
    },
    async stop(): Promise<void> {
      running = false;

      if (timer !== undefined) {
        clearTimer(timer);
        timer = undefined;
      }

      await inFlight;
    }
  };
}

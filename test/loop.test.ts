import {
  afterEach,
  describe,
  expect,
  it,
  vi
} from "vitest";

import { createSchedulerLoop } from "../src/loop.js";
import type { SchedulerService } from "../src/service.js";

afterEach(() => {
  vi.useRealTimers();
});

describe("scheduler loop", () => {
  it("runs immediately, waits one cadence, and stops without another schedule", async () => {
    vi.useFakeTimers();
    const runOnce = vi.fn().mockResolvedValue(undefined);
    const loop = createSchedulerLoop({
      service: {
        runOnce
      } as unknown as SchedulerService,
      cadenceMs: 60_000
    });

    loop.start();
    await vi.advanceTimersByTimeAsync(0);
    expect(runOnce).toHaveBeenCalledTimes(1);

    await vi.advanceTimersByTimeAsync(59_999);
    expect(runOnce).toHaveBeenCalledTimes(1);

    await vi.advanceTimersByTimeAsync(1);
    expect(runOnce).toHaveBeenCalledTimes(2);

    await loop.stop();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(runOnce).toHaveBeenCalledTimes(2);
  });

  it("does not overlap scheduling runs", async () => {
    vi.useFakeTimers();
    let resolveRun: (() => void) | undefined;
    const runOnce = vi.fn(() => new Promise<void>((resolve) => {
      resolveRun = resolve;
    }));
    const loop = createSchedulerLoop({
      service: {
        runOnce
      } as unknown as SchedulerService,
      cadenceMs: 60_000
    });

    loop.start();
    await vi.advanceTimersByTimeAsync(0);
    await vi.advanceTimersByTimeAsync(120_000);
    expect(runOnce).toHaveBeenCalledTimes(1);

    resolveRun?.();
    await Promise.resolve();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(runOnce).toHaveBeenCalledTimes(2);

    resolveRun?.();
    await loop.stop();
  });
});

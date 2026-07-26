import {
  describe,
  expect,
  it
} from "vitest";

import { createSchedulerFailClosedReconciler } from "../src/reconciliation.js";
import { ManualSchedulerClock } from "../src/test-doubles.js";

describe("scheduler reconciliation", () => {
  it("fails closed instead of synthesizing fetch messages from partial metadata", async () => {
    const reconciler = createSchedulerFailClosedReconciler(new ManualSchedulerClock());

    const report = await reconciler.reconcile({
      mode: "dry-run",
      runId: "recovery-20260723"
    });

    expect(report).toMatchObject({
      service: "scheduler",
      status: "failed_closed",
      selectedCount: 0,
      replayedCount: 0,
      writesPerformed: false,
      productionVisibilityEnabled: false,
      legacyRuntimeRequired: false
    });
    expect(report.errors[0]).toContain("refusing to synthesize");
  });
});

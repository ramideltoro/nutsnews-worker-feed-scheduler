import {
  describe,
  expect,
  it
} from "vitest";

import { createSchedulerFailClosedReconciler } from "../src/reconciliation.js";
import { ManualSchedulerClock } from "../src/test-doubles.js";

describe("scheduler reconciliation", () => {
  it("reports a bounded no-op dry-run when no service-owned replay candidates exist", async () => {
    const reconciler = createSchedulerFailClosedReconciler(new ManualSchedulerClock());

    const report = await reconciler.reconcile({
      mode: "dry-run",
      runId: "recovery-20260723"
    });

    expect(report).toMatchObject({
      service: "scheduler",
      status: "dry_run",
      selectedCount: 0,
      replayedCount: 0,
      writesPerformed: false,
      productionVisibilityEnabled: false,
      legacyRuntimeRequired: false
    });
    expect(report.errors).toEqual([]);
  });
});

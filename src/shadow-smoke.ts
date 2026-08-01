import { createBufferedRuntimeTelemetrySink } from "@ramideltoro/nutsnews-worker-runtime";

import { loadSchedulerConfig } from "./config.js";
import {
  SCHEDULER_FIXTURE_FEEDS,
  SCHEDULER_FIXTURE_NOW
} from "./fixtures.js";
import { createSchedulerService } from "./service.js";
import {
  LocalBrokerTransport,
  ManualSchedulerClock,
  createLocalFeedSource
} from "./test-doubles.js";
import { InMemoryScheduleLeaseStore } from "./lease-store.js";

async function main(): Promise<void> {
  const config = loadSchedulerConfig({
    NUTSNEWS_SCHEDULER_TELEMETRY_LOGS: "silent",
    NUTSNEWS_SCHEDULER_HTTP_PORT: "0"
  });
  const telemetry = createBufferedRuntimeTelemetrySink();
  const brokerTransport = new LocalBrokerTransport();
  const service = createSchedulerService({
    config,
    dependencies: {
      mode: "test",
      clockKind: "manual-test",
      brokerKind: "local-test",
      clock: new ManualSchedulerClock(SCHEDULER_FIXTURE_NOW),
      feedSource: createLocalFeedSource({
        feeds: SCHEDULER_FIXTURE_FEEDS
      }),
      leaseStore: new InMemoryScheduleLeaseStore(),
      brokerTransport,
      brokerProbe: brokerTransport
    },
    telemetry
  });

  await service.start();
  const result = await service.runOnce();
  await service.stop();

  process.stdout.write(`${JSON.stringify({
    status: "ok",
    shadowMode: result.shadowMode,
    dueFeedCount: result.dueFeedCount,
    confirmedCount: result.confirmedCount,
    skippedCount: result.skippedCount,
    publishedCount: brokerTransport.published.length,
    telemetryEvents: telemetry.events.length
  })}\n`);
}

main().catch((error: unknown) => {
  process.stderr.write(`${error instanceof Error ? error.message : "shadow smoke failed"}\n`);
  process.exitCode = 1;
});

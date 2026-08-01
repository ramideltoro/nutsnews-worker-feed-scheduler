# Architecture Notes

## Scope

The feed scheduler owns the first worker-uplift service boundary. It leases due feed definitions and publishes fetch work to the contracted `fetch` route. This repository owns the service-local production adapters, scheduling rules, lease contract, message creation, and tests. The backend repository owns their value-free configuration, scoped credentials, immutable image pin, and protected deployment.

## Runtime Surfaces

- Contracts: `@ramideltoro/nutsnews-worker-contracts@0.3.1`
- Runtime: `@ramideltoro/nutsnews-worker-runtime@0.4.0`
- Route boundary: `getWorkerRoute("fetch")`
- Health: separate liveness, startup, and readiness probes
- Metrics: bounded Prometheus text from the shared runtime sink plus service-owned build, mode, loop, expected-activity, last-success, and explicit liveness/startup/readiness signals
- Shutdown: stop accepting work, wait for in-flight operations, close broker lifecycle
- Production adapters: backend Worker DB API feed reader, scheduler-schema PostgreSQL lease store, a publisher-only RabbitMQ adapter built on the shared runtime broker contract, and the system clock

## Scheduling Flow

1. Load active feed definitions from the configured feed source.
2. Evaluate disabled state, backoff state, cadence, and next eligible time with the injected clock.
3. Calculate a stable schedule window from the feed cadence.
4. Build the idempotency key for `feed/window`.
5. Acquire the schedule lease before publishing.
6. Create and validate a `feedFetchRequest` payload and worker envelope.
7. Publish through the runtime broker lifecycle.
8. Mark the lease `confirmed` only after the broker returns a publisher-confirm receipt.
9. Mark failed publishes as retryable failures so the same window can be retried.

The application binds health/metrics and installs shutdown handling before broker initialization and before it launches this flow, so blocked broker startup or a blocked first feed read cannot hide diagnostics. Liveness remains available during initialization while startup and readiness remain unhealthy. It then runs on a recursive cadence and never schedules a second timer until the current iteration settles, preventing overlapping scheduling cycles. Production loop readiness expires after three cadences without a successful cycle.

Confirmed windows are not republished. Active leases suppress duplicate concurrent scheduling until the lease expires. An active or already-confirmed lease does not consume the current cycle's concurrency allowance, so later due feeds can still be considered.

## Concurrency and Recovery

The scheduler treats the schedule-window idempotency key as the cross-replica lock boundary. A replica must acquire the lease before it publishes fetch work, and it finalizes the lease only after the broker returns a publisher-confirm receipt.

The local proof suite exercises simultaneous acquire attempts, stale lease recovery, clock boundaries, UTC/DST behavior, lease-store failures, broker failures, confirm timeouts, non-overlapping loops, production-adapter selection, and shutdown during an in-flight publish without wall-clock sleeps. Integration tests run the production PostgreSQL lease adapter and publisher-confirm service path against PostgreSQL and RabbitMQ when `SCHEDULER_INTEGRATION_POSTGRES_URL` and `SCHEDULER_INTEGRATION_RABBITMQ_URL` are configured.

GitHub Actions starts PostgreSQL and RabbitMQ service containers for CI, so concurrent replicas must produce at most one confirmed schedule-window claim and at most one RabbitMQ message for the tested window. Stale leases recover after the configured lease duration when the injected clock advances beyond the lease expiry.

Failure telemetry includes the feed, window, attempt count when available, idempotency key for lease-store failures, and the dependency label. It does not include connection strings, tokens, URLs from secret configuration, or message bodies.

Telemetry sinks are wrapped independently as best-effort observers before they reach the broker, health probes, or scheduling loop. A rejection from one sink does not starve the other configured sinks and cannot abort publication, strand an acquired lease, or change a confirmed lease to failed; regression coverage rejects both telemetry and metric operations while verifying scheduling and lifecycle state. Health evaluation events remain in logs but are excluded from the legacy runtime metric adapter, leaving the seeded service-owned `nutsnews_worker_health_probe` family as the single health metric contract. Duration-less dependency events are also excluded instead of becoming synthetic zero-millisecond latency; measured `runtime.dependency.observed` events are the sole dependency-latency path.

The deterministic shadow smoke command uses fixture feeds and local doubles:

```sh
npm run smoke:shadow
```

The command returns a compact JSON summary for the explicit test boundary. It is not production dependency evidence.

## Dependency Boundary

Production database, backend API, RabbitMQ, and telemetry credentials stay outside this repository. Backend-owned deployment configuration supplies them only at runtime. The service uses the existing read-only backend feed operation, the stage-owned `worker_uplift_scheduler.feed_leases` table, and the scoped scheduler RabbitMQ publisher URL. Readiness records only fixed adapter names, status, and bounded error classes.

`NUTSNEWS_ENVIRONMENT=production` requires production dependency mode. Production construction uses `SYSTEM_RUNTIME_CLOCK`, `BackendApiFeedSource`, `PostgresScheduleLeaseStore`, and `SchedulerRabbitMqPublisherTransport`; the service boundary rejects manual clocks, local feeds, in-memory leases, and local brokers even if a caller attempts to relabel the bundle.

Production dependency mode also rejects an unknown build revision. Readiness probes the backend feed source, PostgreSQL lease store, RabbitMQ publisher, and system-clock freshness, then requires a recent successful active scheduling cycle and the approved production adapter identities. Configured secrets alone are not proof that the adapters are usable. Shadow deployments export `expected_active=0` for paging ownership without forcing readiness unhealthy, and container health uses liveness rather than ownership state.

## Local Doubles

The repository includes deterministic local doubles for:

- broker transport;
- scheduler clock;
- feed source.

These doubles let tests start the service, expose metrics, and drain cleanly without production dependencies or legacy worker code. They remain available only through explicit test-mode construction and cannot satisfy the production adapter boundary.
These doubles let local/test mode start, become ready, expose metrics, and drain cleanly without production dependencies or legacy worker code. They remain available only through explicit test-mode construction and cannot satisfy the production adapter boundary.

The in-memory lease store also models confirmed, failed, and active-lease behavior for replay/crash-safety tests.

# Architecture Notes

## Scope

The feed scheduler owns the first worker-uplift service boundary. It will eventually lease due feed definitions and publish fetch work to the contracted `fetch` route. The bootstrap shell does not implement scheduling business logic yet; it establishes the deployable runtime surface that #93 can fill in.

## Runtime Surfaces

- Contracts: `@ramideltoro/nutsnews-worker-contracts@0.3.1`
- Runtime: `@ramideltoro/nutsnews-worker-runtime@0.4.0`
- Route boundary: `getWorkerRoute("fetch")`
- Health: separate liveness, startup, and readiness probes
- Metrics: bounded Prometheus text from the shared runtime sink
- Shutdown: stop accepting work, wait for in-flight operations, close broker lifecycle

## Dependency Boundary

Production database, backend API, RabbitMQ, and telemetry credentials stay outside this repository. The service stores only whether dependency variables are configured, not the secret values themselves. Backend-owned deployment configuration supplies real values later.

## Local Doubles

The repository includes deterministic local doubles for:

- broker transport;
- scheduler clock;
- feed source.

These doubles let the empty service start, become ready, expose metrics, and drain cleanly without production dependencies or legacy worker code.

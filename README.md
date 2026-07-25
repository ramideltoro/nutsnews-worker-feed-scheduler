# nutsnews-worker-feed-scheduler

Deployable worker-uplift feed scheduler service shell for NutsNews.

## Responsibility

Own the scheduler service boundary that will read active feed definitions, decide which feeds are due, and publish validated fetch work without touching legacy ingestion.

Issue #92 bootstrapped the deployable shell. Issue #93 adds due-feed selection, idempotent schedule-window leasing, contract-valid fetch request publication, and confirmation-only finalization.

## Owner

@ramideltoro

## Deployable / Package Type

Containerized worker service image: `ghcr.io/ramideltoro/nutsnews-worker-feed-scheduler:${GITHUB_SHA}`. This repository is deployable only through backend-owned infrastructure.

The image runs as a non-root user, exposes port `8080`, and serves:

- `GET /live`
- `GET /startup`
- `GET /ready`
- `GET /metrics`
- `GET /config-schema`

## Runtime Dependencies

The service consumes exact immutable worker-uplift package versions:

- `@ramideltoro/nutsnews-worker-contracts@0.3.1`
- `@ramideltoro/nutsnews-worker-runtime@0.4.0`

Local and CI installs use the owner-scoped GitHub Packages npm registry. No package token value is committed.

## Configuration

The value-free configuration schema lives in `src/config.ts` and is exposed at `/config-schema`. Production deployments must provide dependency values through backend-owned deployment configuration, not this repository.

Important variables:

- `NUTSNEWS_SCHEDULER_DEPENDENCY_MODE`: `test` or `production`
- `NUTSNEWS_SCHEDULER_DATABASE_URL`
- `NUTSNEWS_SCHEDULER_BACKEND_API_URL`
- `NUTSNEWS_SCHEDULER_BACKEND_API_TOKEN`
- `NUTSNEWS_SCHEDULER_RABBITMQ_URL`
- `NUTSNEWS_SCHEDULER_CADENCE_MS`
- `NUTSNEWS_SCHEDULER_LEASE_MS`
- `NUTSNEWS_SCHEDULER_CONCURRENCY`
- `NUTSNEWS_SCHEDULER_SHADOW_MODE`

`NUTSNEWS_SCHEDULER_SHADOW_MODE` must remain `true` until backend-owned cutover work explicitly changes the deployment contract.

## Scheduling Behavior

The scheduler evaluates active feed definitions with an injectable clock:

- disabled feeds are skipped with reason `disabled`;
- feeds in backoff are skipped with reason `backoff`;
- feeds whose cadence has not elapsed are skipped with reason `not_due`;
- due feeds are ordered by priority, then feed ID;
- each feed/window uses a stable idempotency key: `scheduler:feed:<feed-id>:<window>`;
- the lease store is acquired before RabbitMQ publish;
- the lease is finalized as `confirmed` only after publisher confirmation;
- failed publishes mark the lease `failed`, allowing a later retry;
- already confirmed windows are not published again.

Published fetch requests validate against `@ramideltoro/nutsnews-worker-contracts@0.3.1`. Schedule-window metadata is carried inside the payload `limits` object while correlation and idempotency live on the worker envelope.

## Development

```sh
export NODE_AUTH_TOKEN="<GitHub classic PAT with read:packages>"
npm ci
npm run ci
docker build --secret id=npm_token,env=NODE_AUTH_TOKEN -t nutsnews-worker-feed-scheduler:local .
```

`npm run ci` runs linting, strict type checking, unit tests, integration tests, build, CycloneDX SBOM generation, and a production dependency audit.

## Support Boundary

This repository owns its package or service implementation, CI, package or image publishing workflow, and service-local operational notes. It does not own the backend host, production deployment secrets, Grafana Cloud resources, or cross-system explanatory documentation.

## Production Boundary

`ramideltoro/nutsnews-backend` owns backend-host runtime and deployments. `production-backend` in that repository remains the runtime secret and deployment boundary. No production secret belongs in this repository.

`ramideltoro/nutsnews-infra` owns Grafana Cloud resources. `ramideltoro/nutsnews-docs` owns explanatory architecture and operations documentation.

## Package / Image Access

Backend deployments consume immutable SHA-tagged GHCR images. The only intended production package consumer is `ramideltoro/nutsnews-backend/.github/workflows/protected-backend-ansible-apply.yml` with `packages: read`.

No long-lived GitHub Packages token is required for CI when package access is granted to this repository. Workflows use least-privilege permissions, request `packages: read` for package install jobs, and request `packages: write` only for image publish jobs.

## Guardrail

This repository must not modify, disable, or depend on the active legacy `ramideltoro/nutsnews-worker` ingestion or failover path.

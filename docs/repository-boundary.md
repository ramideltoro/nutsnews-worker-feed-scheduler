# Repository Boundary

| Field | Value |
| --- | --- |
| Repository | `ramideltoro/nutsnews-worker-feed-scheduler` |
| Owner | `@ramideltoro` |
| Responsibility | Read active feed definitions, decide which feeds are due, and publish validated rss.feed.fetch messages without touching legacy ingestion. |
| Deployable / package type | Deployable service repo. Publishes immutable SHA-tagged images to `ghcr.io/ramideltoro/nutsnews-worker-feed-scheduler`. |
| Primary artifact | Signed GHCR image tagged only by commit SHA |
| Support boundary | Repo-local code, tests, CI, package/image publishing, and service-local run notes. |
| Outside boundary | Backend host runtime/deployments, Grafana Cloud resources, explanatory architecture/operations docs, production secrets, legacy ingestion. |

## Existing Ownership Boundaries

- `ramideltoro/nutsnews-backend` owns backend-host runtime and deployments.
- `production-backend` in `ramideltoro/nutsnews-backend` remains the runtime secret and deployment boundary.
- `ramideltoro/nutsnews-infra` owns Grafana Cloud resources.
- `ramideltoro/nutsnews-docs` owns explanatory architecture and operations docs.
- `ramideltoro/nutsnews-worker` remains the active legacy ingestion and failover path until explicit cutover.

## Control Baseline

- Public repository with MIT license to match existing NutsNews worker/docs visibility and licensing.
- Default branch: `main`.
- CODEOWNERS assigns repository ownership to `@ramideltoro`.
- Default Actions token permission: read-only.
- Publish workflows request package write permission only inside publish jobs.
- CI validates repo boundary docs on every push and pull request.
- CI runs lint, type checks, unit tests, integration tests, build, SBOM generation, production dependency audit, and local container build.
- CodeQL and dependency review workflows are present for code-bearing changes.
- Dependabot checks GitHub Actions and npm manifests.
- Branch protection requires pull requests, resolved conversations, and the `validate` status check where GitHub permits repository branch protection. CODEOWNERS documents ownership for reviews.

## Conditional Controls

Some GitHub package access controls can be applied only after the first package or container image exists. Until then, this repository documents the intended access model and publish workflows:

- Package repos publish exact versions to GitHub Packages. Downstream repos install through `GITHUB_TOKEN` with `packages: read`.
- Service repos publish signed, immutable SHA-tagged images to GHCR. Backend production deploys pull through `ramideltoro/nutsnews-backend/.github/workflows/protected-backend-ansible-apply.yml` only.
- App repos must not define production environments or store production runtime secrets.

## Scheduler Shell

This repository currently owns the deployable shell for the scheduler stage:

- exact package pins to `@ramideltoro/nutsnews-worker-contracts@1.0.0` and `@ramideltoro/nutsnews-worker-runtime@1.0.0`;
- liveness, startup, readiness, metrics, and value-free config-schema endpoints;
- graceful shutdown using the shared runtime drain controller;
- backend API feed source, PostgreSQL-server-clock-authoritative scheduler leases, and bounded RabbitMQ publisher confirms in production mode;
- local broker, manual clock, feed-source, and in-memory lease doubles for explicit test mode only;
- shadow-mode enforcement so this service cannot cut over legacy ingestion by configuration alone.
- due-feed scheduling decisions, idempotent schedule-window leases, contract-valid fetch-work publication, and publisher-confirm finalization.
- an active non-overlapping scheduling loop, production-adapter and loop-freshness readiness gates, Runtime 1.0 bounded build/mode/expected-active/last-success/health metrics, and scheduler-owned loop/cycle metrics; expected-active controls paging ownership, not readiness.
